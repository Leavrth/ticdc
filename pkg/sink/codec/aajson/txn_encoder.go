// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package aajson

import (
	"bytes"
	"crypto/sha256"
	"sort"

	"github.com/goccy/go-json"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

// Message is the compact JSON payload for active-active sink.
type Message struct {
	PkData   []map[string]any `json:"pkData"`
	CommitTs uint64           `json:"commitTs"`
	OriginTs uint64           `json:"originTs"`
	Checksum []byte           `json:"checksum"`
}

type checksumEntry struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

// TxnEventEncoder encodes txn event in aa-json format.
type TxnEventEncoder struct {
	config *common.Config

	terminator []byte
	valueBuf   *bytes.Buffer
	batchSize  int
	callback   func()
}

// NewTxnEventEncoder creates a new aa-json txn encoder.
func NewTxnEventEncoder(config *common.Config) common.TxnEventEncoder {
	return &TxnEventEncoder{
		config:     config,
		terminator: []byte(config.Terminator),
		valueBuf:   &bytes.Buffer{},
	}
}

// AppendTxnEvent appends a txn event to the encoder.
func (e *TxnEventEncoder) AppendTxnEvent(event *commonEvent.DMLEvent) error {
	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}

		value, err := encodeMessage(event.TableInfo, event.CommitTs, row)
		if err != nil {
			return errors.Trace(err)
		}
		if len(value) == 0 {
			// delete rows are skipped for aa-json
			continue
		}

		length := len(value) + common.MaxRecordOverhead
		if length > e.config.MaxMessageBytes {
			log.Warn("Single message is too large for aa json",
				zap.Int("maxMessageBytes", e.config.MaxMessageBytes),
				zap.Int("length", length),
				zap.Any("table", event.TableInfo.TableName))
			return errors.ErrMessageTooLarge.GenWithStackByArgs(event.TableInfo.GetTableName(), length, e.config.MaxMessageBytes)
		}

		e.valueBuf.Write(value)
		e.valueBuf.Write(e.terminator)
		e.batchSize++
	}
	e.callback = event.PostFlush
	return nil
}

// Build builds a message from the encoder and resets the encoder.
func (e *TxnEventEncoder) Build() []*common.Message {
	if e.batchSize == 0 {
		return nil
	}

	ret := common.NewMsg(nil, e.valueBuf.Bytes())
	ret.SetRowsCount(e.batchSize)
	ret.Callback = e.callback

	if e.valueBuf.Cap() > common.MemBufShrinkThreshold {
		e.valueBuf = &bytes.Buffer{}
	} else {
		e.valueBuf.Reset()
	}
	e.batchSize = 0
	e.callback = nil

	return []*common.Message{ret}
}

func encodeMessage(tableInfo *commonType.TableInfo, commitTs uint64, row commonEvent.RowChange) ([]byte, error) {
	currentRow := getCurrentRow(row)
	if currentRow == nil {
		return nil, nil
	}
	if currentRow.IsEmpty() {
		return nil, errors.New("invalid row for aa json encoding")
	}

	pkMap := make(map[string]any)
	checksumEntries := make([]checksumEntry, 0, len(tableInfo.GetColumns()))
	originTs := uint64(0)

	for idx, col := range tableInfo.GetColumns() {
		if col == nil || col.IsVirtualGenerated() {
			continue
		}

		datum := currentRow.GetDatum(idx, &col.FieldType)
		if col.Name.O == commonEvent.OriginTsColumn {
			originTs = datumToOriginTs(datum)
			continue
		}

		value, err := datumToEncodedValue(datum)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if isPrimaryKeyColumn(col) {
			pkMap[col.Name.O] = value
			continue
		}
		checksumEntries = append(checksumEntries, checksumEntry{
			Name:  col.Name.O,
			Value: value,
		})
	}

	checksum, err := calculateChecksum(checksumEntries)
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg := Message{
		PkData:   []map[string]any{pkMap},
		CommitTs: commitTs,
		OriginTs: originTs,
		Checksum: checksum,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return payload, nil
}

func calculateChecksum(entries []checksumEntry) ([]byte, error) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
	payload, err := json.Marshal(entries)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sum := sha256.Sum256(payload)
	return sum[:], nil
}

func getCurrentRow(row commonEvent.RowChange) *chunk.Row {
	switch row.RowType {
	case commonType.RowTypeDelete:
		return nil
	default:
		return &row.Row
	}
}

func isPrimaryKeyColumn(col *model.ColumnInfo) bool {
	return mysql.HasPriKeyFlag(col.GetFlag())
}

func datumToEncodedValue(d types.Datum) (any, error) {
	if d.IsNull() {
		return nil, nil
	}
	value, err := d.ToString()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return value, nil
}

func datumToOriginTs(d types.Datum) uint64 {
	if d.IsNull() {
		return 0
	}

	return d.GetUint64()
}

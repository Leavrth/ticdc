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
	"strings"
	"testing"

	"github.com/goccy/go-json"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestBuildAAJSONTxnEventEncoder(t *testing.T) {
	cfg := codecCommon.NewConfig(config.ProtocolAAJSON)
	encoder := NewTxnEventEncoder(cfg)
	require.NotNil(t, encoder)
}

func TestAAJSONEncodeSemantics(t *testing.T) {
	tableInfo := buildTableInfo()

	t.Run("encodes origin and checksum", func(t *testing.T) {
		event := buildInsertDMLEvent(tableInfo, 100, []map[string]any{
			{"id": int64(1), "val": []byte("v1"), "_tidb_origin_ts": int64(123)},
		})

		cfg := codecCommon.NewConfig(config.ProtocolAAJSON)
		cfg.Terminator = "\n"
		encoder := NewTxnEventEncoder(cfg)
		err := encoder.AppendTxnEvent(event)
		require.NoError(t, err)

		msgs := encoder.Build()
		require.Len(t, msgs, 1)
		records := decodeMessages(t, msgs[0].Value)
		require.Len(t, records, 1)

		require.Equal(t, uint64(100), records[0].CommitTs)
		require.Equal(t, uint64(123), records[0].OriginTs)
		require.NotEmpty(t, records[0].Checksum)
		require.Equal(t, "1", records[0].PkData[0]["id"])
	})

	t.Run("checksum excludes pk and origin ts", func(t *testing.T) {
		event := buildInsertDMLEvent(tableInfo, 101, []map[string]any{
			{"id": int64(1), "val": []byte("same"), "_tidb_origin_ts": int64(111)},
			{"id": int64(2), "val": []byte("same"), "_tidb_origin_ts": int64(222)},
		})

		cfg := codecCommon.NewConfig(config.ProtocolAAJSON)
		cfg.Terminator = "\n"
		encoder := NewTxnEventEncoder(cfg)
		err := encoder.AppendTxnEvent(event)
		require.NoError(t, err)

		msgs := encoder.Build()
		require.Len(t, msgs, 1)
		records := decodeMessages(t, msgs[0].Value)
		require.Len(t, records, 2)

		require.True(t, bytes.Equal(records[0].Checksum, records[1].Checksum))
	})

	t.Run("checksum includes non pk columns", func(t *testing.T) {
		event := buildInsertDMLEvent(tableInfo, 102, []map[string]any{
			{"id": int64(1), "val": []byte("v1"), "_tidb_origin_ts": int64(111)},
			{"id": int64(2), "val": []byte("v2"), "_tidb_origin_ts": int64(222)},
		})

		cfg := codecCommon.NewConfig(config.ProtocolAAJSON)
		cfg.Terminator = "\n"
		encoder := NewTxnEventEncoder(cfg)
		err := encoder.AppendTxnEvent(event)
		require.NoError(t, err)

		msgs := encoder.Build()
		require.Len(t, msgs, 1)
		records := decodeMessages(t, msgs[0].Value)
		require.Len(t, records, 2)

		require.False(t, bytes.Equal(records[0].Checksum, records[1].Checksum))
	})

	t.Run("skip delete rows", func(t *testing.T) {
		event := buildDeleteDMLEvent(tableInfo, 103, []map[string]any{
			{"id": int64(1), "val": []byte("v1"), "_tidb_origin_ts": int64(111)},
		})

		cfg := codecCommon.NewConfig(config.ProtocolAAJSON)
		cfg.Terminator = "\n"
		encoder := NewTxnEventEncoder(cfg)
		err := encoder.AppendTxnEvent(event)
		require.NoError(t, err)

		msgs := encoder.Build()
		require.Nil(t, msgs)
	})
}

func buildTableInfo() *commonType.TableInfo {
	idCol := &model.ColumnInfo{
		ID:        1,
		Offset:    0,
		State:     model.StatePublic,
		Name:      ast.NewCIStr("id"),
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	idCol.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)

	valCol := &model.ColumnInfo{
		ID:        2,
		Offset:    1,
		State:     model.StatePublic,
		Name:      ast.NewCIStr("val"),
		FieldType: *types.NewFieldType(mysql.TypeVarchar),
	}
	originTsCol := &model.ColumnInfo{
		ID:        3,
		Offset:    2,
		State:     model.StatePublic,
		Name:      ast.NewCIStr(commonEvent.OriginTsColumn),
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}

	tableInfo := &model.TableInfo{
		ID:         1,
		Name:       ast.NewCIStr("t"),
		Columns:    []*model.ColumnInfo{idCol, valCol, originTsCol},
		PKIsHandle: true,
	}
	return commonType.WrapTableInfo("test", tableInfo)
}

func buildInsertDMLEvent(tableInfo *commonType.TableInfo, commitTs uint64, rows []map[string]any) *commonEvent.DMLEvent {
	event := commonEvent.NewDMLEvent(commonType.NewDispatcherID(), 1, 1, commitTs, tableInfo)
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), len(rows))
	for _, row := range rows {
		codecCommon.AppendRow2Chunk(row, tableInfo.GetColumns(), chk)
		event.RowTypes = append(event.RowTypes, commonType.RowTypeInsert)
	}
	event.SetRows(chk)
	event.Length = int32(len(rows))
	return event
}

func buildDeleteDMLEvent(tableInfo *commonType.TableInfo, commitTs uint64, rows []map[string]any) *commonEvent.DMLEvent {
	event := commonEvent.NewDMLEvent(commonType.NewDispatcherID(), 1, 1, commitTs, tableInfo)
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), len(rows))
	for _, row := range rows {
		codecCommon.AppendRow2Chunk(row, tableInfo.GetColumns(), chk)
		event.RowTypes = append(event.RowTypes, commonType.RowTypeDelete)
	}
	event.SetRows(chk)
	event.Length = int32(len(rows))
	return event
}

func decodeMessages(t *testing.T, value []byte) []Message {
	lines := strings.Split(strings.TrimSpace(string(value)), "\n")
	result := make([]Message, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		var message Message
		err := json.Unmarshal([]byte(line), &message)
		require.NoError(t, err)
		result = append(result, message)
	}
	return result
}

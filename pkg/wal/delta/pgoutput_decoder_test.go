// SPDX-License-Identifier: Apache-2.0

package delta

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	replicationpg "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func TestPGOutputDecoder_Insert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	decoder := NewPGOutputDecoder(&staticTypeMapper{
		types: map[uint32]string{
			23: "int4",
			25: "text",
		},
	}, replicationpg.NewLSNParser())

	require.NoError(t, decoder.registerRelation(ctx, &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "test",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},
			{Name: "name", DataType: 25},
		},
	}))

	decoder.txn.commitTime = time.Unix(0, 0).UTC()

	events, err := decoder.handleLogicalMessage(ctx, &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple: &pglogrepl.TupleData{
			ColumnNum: 2,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("alice")},
			},
		},
	}, 0x16, 0x10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "I", events[0].Data.Action)
	require.Equal(t, int32(1), events[0].Data.Columns[0].Value)
	require.Equal(t, "alice", events[0].Data.Columns[1].Value)
	require.Equal(t, "0/16", string(events[0].CommitPosition))
}

func TestPGOutputDecoder_UpdateSkipsToast(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	decoder := NewPGOutputDecoder(&staticTypeMapper{
		types: map[uint32]string{
			23: "int4",
			25: "text",
		},
	}, replicationpg.NewLSNParser())

	require.NoError(t, decoder.registerRelation(ctx, &pglogrepl.RelationMessage{
		RelationID:   2,
		Namespace:    "public",
		RelationName: "test",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},
			{Name: "name", DataType: 25},
		},
	}))

	decoder.txn.commitTime = time.Now().UTC()

	events, err := decoder.handleLogicalMessage(ctx, &pglogrepl.UpdateMessage{
		RelationID: 2,
		OldTuple: &pglogrepl.TupleData{
			ColumnNum: 1,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
			},
		},
		NewTuple: &pglogrepl.TupleData{
			ColumnNum: 2,
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
				{DataType: pglogrepl.TupleDataTypeToast},
			},
		},
	}, 0x20, 0x18)
	require.NoError(t, err)
	require.Len(t, events, 1)
	update := events[0]
	require.Len(t, update.Data.Columns, 1, "unchanged toast column should be skipped")
	require.Equal(t, "id", update.Data.Columns[0].Name)
	require.Equal(t, int32(1), update.Data.Columns[0].Value)
	require.Len(t, update.Data.Identity, 1)
	require.Equal(t, "id", update.Data.Identity[0].Name)
	require.Equal(t, int32(1), update.Data.Identity[0].Value)
}

type staticTypeMapper struct {
	types map[uint32]string
}

func (m *staticTypeMapper) TypeForOID(_ context.Context, oid uint32) (string, error) {
	if val, ok := m.types[oid]; ok {
		return val, nil
	}
	return "", fmt.Errorf("unknown oid %d", oid)
}

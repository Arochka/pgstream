// SPDX-License-Identifier: Apache-2.0

package delta

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

type typeNameMapper interface {
	TypeForOID(context.Context, uint32) (string, error)
}

// MessageDecoder converts replication messages into wal events.
type MessageDecoder interface {
	Decode(ctx context.Context, msg *pglib.ReplicationMessage) ([]*wal.Event, error)
}

type relationInfo struct {
	schema string
	table  string
	cols   []relationColumn
}

type relationColumn struct {
	name    string
	oid     uint32
	typeStr string
	dt      *pgtype.Type
}

type txnState struct {
	commitTime time.Time
	finalLSN   string
}

// PGOutputDecoder converts pgoutput logical replication messages into wal.Events.
type PGOutputDecoder struct {
	typeMap   *pgtype.Map
	mapper    typeNameMapper
	lsnParser replication.LSNParser

	relations map[uint32]*relationInfo
	txn       txnState
}

func NewPGOutputDecoder(mapper typeNameMapper, parser replication.LSNParser) *PGOutputDecoder {
	return &PGOutputDecoder{
		typeMap:   pgtype.NewMap(),
		mapper:    mapper,
		lsnParser: parser,
		relations: make(map[uint32]*relationInfo),
	}
}

func (d *PGOutputDecoder) Decode(ctx context.Context, msg *pglib.ReplicationMessage) ([]*wal.Event, error) {
	if msg == nil || msg.WALData == nil {
		return nil, nil
	}
	logical, err := pglogrepl.Parse(msg.WALData)
	if err != nil {
		return nil, fmt.Errorf("parse logical replication message: %w", err)
	}
	walStart := msg.LSN
	if msg.LSN >= uint64(len(msg.WALData)) {
		walStart -= uint64(len(msg.WALData))
	} else {
		walStart = 0
	}
	return d.handleLogicalMessage(ctx, logical, msg.LSN, walStart)
}

func (d *PGOutputDecoder) handleLogicalMessage(ctx context.Context, logical pglogrepl.Message, lsn uint64, walStart uint64) ([]*wal.Event, error) {
	switch m := logical.(type) {
	case *pglogrepl.RelationMessage:
		return nil, d.registerRelation(ctx, m)
	case *pglogrepl.BeginMessage:
		d.txn.commitTime = m.CommitTime.UTC()
		d.txn.finalLSN = d.lsnParser.ToString(replication.LSN(m.FinalLSN))
		return nil, nil
	case *pglogrepl.CommitMessage:
		d.txn = txnState{}
		return nil, nil
	case *pglogrepl.InsertMessage:
		return d.insertEvent(ctx, m, lsn, walStart)
	case *pglogrepl.UpdateMessage:
		return d.updateEvent(ctx, m, lsn, walStart)
	case *pglogrepl.DeleteMessage:
		return d.deleteEvent(ctx, m, lsn, walStart)
	case *pglogrepl.TruncateMessage:
		return d.truncateEvents(m, lsn, walStart)
	default:
		// ignore other message types
		return nil, nil
	}
}

func (d *PGOutputDecoder) registerRelation(ctx context.Context, msg *pglogrepl.RelationMessage) error {
	cols := make([]relationColumn, 0, len(msg.Columns))
	for _, column := range msg.Columns {
		typeStr, err := d.mapper.TypeForOID(ctx, column.DataType)
		if err != nil {
			return fmt.Errorf("fetching type for oid %d: %w", column.DataType, err)
		}
		dataType, _ := d.typeMap.TypeForOID(column.DataType)
		cols = append(cols, relationColumn{
			name:    column.Name,
			oid:     column.DataType,
			typeStr: typeStr,
			dt:      dataType,
		})
	}
	d.relations[msg.RelationID] = &relationInfo{
		schema: msg.Namespace,
		table:  msg.RelationName,
		cols:   cols,
	}
	return nil
}

func (d *PGOutputDecoder) insertEvent(ctx context.Context, msg *pglogrepl.InsertMessage, lsn uint64, walStart uint64) ([]*wal.Event, error) {
	rel, err := d.lookupRelation(msg.RelationID)
	if err != nil {
		return nil, err
	}
	columns, err := d.decodeTuple(ctx, rel, msg.Tuple, true)
	if err != nil {
		return nil, err
	}
	event := d.newEvent(rel, "I", columns, nil, lsn, walStart)
	return []*wal.Event{event}, nil
}

func (d *PGOutputDecoder) updateEvent(ctx context.Context, msg *pglogrepl.UpdateMessage, lsn uint64, walStart uint64) ([]*wal.Event, error) {
	rel, err := d.lookupRelation(msg.RelationID)
	if err != nil {
		return nil, err
	}
	columns, err := d.decodeTuple(ctx, rel, msg.NewTuple, true)
	if err != nil {
		return nil, err
	}
	identity, err := d.decodeTuple(ctx, rel, msg.OldTuple, false)
	if err != nil {
		return nil, err
	}
	event := d.newEvent(rel, "U", columns, identity, lsn, walStart)
	return []*wal.Event{event}, nil
}

func (d *PGOutputDecoder) deleteEvent(ctx context.Context, msg *pglogrepl.DeleteMessage, lsn uint64, walStart uint64) ([]*wal.Event, error) {
	rel, err := d.lookupRelation(msg.RelationID)
	if err != nil {
		return nil, err
	}
	identity, err := d.decodeTuple(ctx, rel, msg.OldTuple, false)
	if err != nil {
		return nil, err
	}
	event := d.newEvent(rel, "D", nil, identity, lsn, walStart)
	return []*wal.Event{event}, nil
}

func (d *PGOutputDecoder) truncateEvents(msg *pglogrepl.TruncateMessage, lsn uint64, walStart uint64) ([]*wal.Event, error) {
	if msg == nil {
		return nil, nil
	}
	events := make([]*wal.Event, 0, len(msg.RelationIDs))
	for _, relID := range msg.RelationIDs {
		rel, err := d.lookupRelation(relID)
		if err != nil {
			return nil, err
		}
		event := d.newEvent(rel, "T", nil, nil, lsn, walStart)
		events = append(events, event)
	}
	return events, nil
}

func (d *PGOutputDecoder) lookupRelation(relID uint32) (*relationInfo, error) {
	rel, ok := d.relations[relID]
	if !ok {
		return nil, fmt.Errorf("relation %d not registered", relID)
	}
	return rel, nil
}

func (d *PGOutputDecoder) decodeTuple(ctx context.Context, rel *relationInfo, tuple *pglogrepl.TupleData, skipToast bool) ([]wal.Column, error) {
	if tuple == nil {
		return nil, nil
	}
	if len(tuple.Columns) > len(rel.cols) {
		return nil, fmt.Errorf("tuple has more columns (%d) than relation definition (%d)", len(tuple.Columns), len(rel.cols))
	}
	cols := make([]wal.Column, 0, len(tuple.Columns))
	for idx, colData := range tuple.Columns {
		relCol := rel.cols[idx]
		switch colData.DataType {
		case pglogrepl.TupleDataTypeNull:
			cols = append(cols, wal.Column{
				Name:  relCol.name,
				Type:  relCol.typeStr,
				Value: nil,
			})
		case pglogrepl.TupleDataTypeToast:
			if skipToast {
				continue
			}
			cols = append(cols, wal.Column{
				Name:  relCol.name,
				Type:  relCol.typeStr,
				Value: wal.UnchangedToastValue(),
			})
		case pglogrepl.TupleDataTypeText, pglogrepl.TupleDataTypeBinary:
			value, err := d.decodeValue(relCol, colData)
			if err != nil {
				return nil, err
			}
			cols = append(cols, wal.Column{
				Name:  relCol.name,
				Type:  relCol.typeStr,
				Value: value,
			})
		default:
			return nil, fmt.Errorf("unsupported tuple data type %d", colData.DataType)
		}
	}
	return cols, nil
}

func (d *PGOutputDecoder) decodeValue(col relationColumn, data *pglogrepl.TupleDataColumn) (any, error) {
	format := int16(pgx.TextFormatCode)
	if data.DataType == pglogrepl.TupleDataTypeBinary {
		format = int16(pgx.BinaryFormatCode)
	}
	if col.dt == nil || col.dt.Codec == nil {
		if format == pgx.TextFormatCode {
			return string(data.Data), nil
		}
		buf := make([]byte, len(data.Data))
		copy(buf, data.Data)
		return buf, nil
	}
	value, err := col.dt.Codec.DecodeValue(d.typeMap, col.oid, format, data.Data)
	if err != nil {
		return nil, fmt.Errorf("decode column %s: %w", col.name, err)
	}
	return value, nil
}

func (d *PGOutputDecoder) newEvent(rel *relationInfo, action string, columns []wal.Column, identity []wal.Column, lsn uint64, walStart uint64) *wal.Event {
	commitPos := d.lsnParser.ToString(replication.LSN(lsn))
	dataLSN := d.lsnParser.ToString(replication.LSN(walStart))
	timestamp := d.txn.commitTime
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	return &wal.Event{
		CommitPosition: wal.CommitPosition(commitPos),
		Data: &wal.Data{
			Action:    action,
			Schema:    rel.schema,
			Table:     rel.table,
			Columns:   columns,
			Identity:  identity,
			LSN:       dataLSN,
			Timestamp: timestamp.Format(time.RFC3339Nano),
		},
	}
}

// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/store"
)

type Store struct {
	conn postgres.Querier
}

const queryLimit = 1000

func New(ctx context.Context, url string) (*Store, error) {
	conn, err := postgres.NewConnPool(ctx, url)
	if err != nil {
		return nil, err
	}

	s := &Store{
		conn: conn,
	}

	// create snapshots table if it doesn't exist
	if err := s.createTable(ctx); err != nil {
		return nil, fmt.Errorf("creating snapshots table: %w", err)
	}

	return s, nil
}

func (s *Store) Close() error {
	return s.conn.Close(context.Background())
}

func (s *Store) CreateSnapshotRequest(ctx context.Context, req *snapshot.Request) error {
	mode := req.Mode
	if mode == "" {
		mode = snapshot.ModeFull
	}

	if mode != snapshot.ModeDelta {
		cleanupQuery := fmt.Sprintf(`DELETE FROM %s WHERE schema_name = $1 AND table_names = $2 AND status != 'completed'`, snapshotsTable())
		if _, err := s.conn.Exec(ctx, cleanupQuery, req.Schema, pq.StringArray(req.Tables)); err != nil {
			return fmt.Errorf("cleaning up pending snapshot request: %w", err)
		}
	}

	query := fmt.Sprintf(`INSERT INTO %s (schema_name, table_names, mode, start_lsn, end_lsn, completed_tables, completed_table_lsns, created_at, updated_at, status)
	VALUES($1, $2, $3, $4, $5, $6, $7, now(), now(),'requested')`, snapshotsTable())
	completedTables := pq.StringArray(req.CompletedTables)
	if completedTables == nil {
		completedTables = pq.StringArray{}
	}
	completedLSNsBytes, err := json.Marshal(nonNilMap(req.CompletedTableLSNs))
	if err != nil {
		return fmt.Errorf("marshalling completed table lsns: %w", err)
	}
	_, err = s.conn.Exec(ctx, query, req.Schema, pq.StringArray(req.Tables), mode, nullString(req.StartLSN), nullString(req.EndLSN), completedTables, completedLSNsBytes)
	if err != nil {
		return fmt.Errorf("error creating snapshot request: %w", err)
	}

	return nil
}

func (s *Store) UpdateSnapshotRequest(ctx context.Context, req *snapshot.Request) error {
	mode := req.Mode
	if mode == "" {
		mode = snapshot.ModeFull
	}
	query := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, completed_tables = COALESCE($3, completed_tables), completed_table_lsns = COALESCE($4, completed_table_lsns), mode = COALESCE($5, mode), start_lsn = COALESCE($6, start_lsn), end_lsn = COALESCE($7, end_lsn), updated_at = now()
	WHERE schema_name = $8 AND table_names = $9 and status != 'completed'`, snapshotsTable())

	var completedTables interface{}
	if req.CompletedTables != nil {
		completedTables = pq.StringArray(req.CompletedTables)
	}

	var completedLSNs interface{}
	if req.CompletedTableLSNs != nil {
		completedLSNsBytes, err := json.Marshal(req.CompletedTableLSNs)
		if err != nil {
			return fmt.Errorf("marshalling completed table lsns: %w", err)
		}
		completedLSNs = completedLSNsBytes
	}

	var modeValue interface{}
	if mode != "" {
		modeValue = mode
	}
	var startLSN interface{}
	if req.StartLSN != "" {
		startLSN = req.StartLSN
	}
	var endLSN interface{}
	if req.EndLSN != "" {
		endLSN = req.EndLSN
	}

	_, err := s.conn.Exec(ctx, query, req.Status, req.Errors, completedTables, completedLSNs, modeValue, startLSN, endLSN, req.Schema, pq.StringArray(req.Tables))
	if err != nil {
		return fmt.Errorf("error updating snapshot request: %w", err)
	}

	return nil
}

func (s *Store) MarkTableCompleted(ctx context.Context, schema, table, lsn string) error {
	query := fmt.Sprintf(`UPDATE %s
SET completed_tables = (
	SELECT COALESCE(array_agg(elem), '{}')
	FROM (
		SELECT DISTINCT elem
		FROM unnest(array_append(completed_tables, $1)) AS elem
	) AS dedup
),
	completed_table_lsns = COALESCE(completed_table_lsns, '{}'::jsonb) || jsonb_build_object($1, $3::text),
	updated_at = now()
WHERE (schema_name = $2 OR schema_name = '*')
	AND status != 'completed'
	AND (
		array_position(table_names, $1) IS NOT NULL
		OR array_position(table_names, '*') IS NOT NULL
		OR COALESCE(array_length(table_names, 1), 0) = 0
	)`, snapshotsTable())
	if _, err := s.conn.Exec(ctx, query, table, schema, lsn); err != nil {
		return fmt.Errorf("error marking table %s.%s completed: %w", schema, table, err)
	}
	return nil
}

func (s *Store) GetSnapshotRequestsByStatus(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
	query := fmt.Sprintf(`SELECT schema_name,
	table_names,
	mode,
	start_lsn,
	end_lsn,
	COALESCE(completed_tables, '{}'::text[])::text AS completed_tables_raw,
	COALESCE(completed_table_lsns, '{}'::jsonb),
	status,
	errors FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), status, queryLimit)
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot requests by status: %w", err)
	}
	defer rows.Close()

	snapshotRequests := []*snapshot.Request{}
	for rows.Next() {
		req := &snapshot.Request{}
		var completedTables pq.StringArray
		var completedTablesRaw sql.NullString
		var completedTableLSNsBytes []byte
		var mode sql.NullString
		var startLSN sql.NullString
		var endLSN sql.NullString
		if err := rows.Scan(&req.Schema, &req.Tables, &mode, &startLSN, &endLSN, &completedTablesRaw, &completedTableLSNsBytes, &req.Status, &req.Errors); err != nil {
			return nil, fmt.Errorf("scanning snapshot row: %w", err)
		}
		if mode.Valid {
			req.Mode = snapshot.Mode(mode.String)
		}
		if startLSN.Valid {
			req.StartLSN = startLSN.String
		}
		if endLSN.Valid {
			req.EndLSN = endLSN.String
		}
		if completedTablesRaw.Valid {
			if err := completedTables.Scan(completedTablesRaw.String); err != nil {
				return nil, fmt.Errorf("decoding completed tables: %w", err)
			}
		}
		req.CompletedTables = completedTables
		req.CompletedTableLSNs = make(map[string]string)
		if len(completedTableLSNsBytes) > 0 && !bytes.Equal(bytes.TrimSpace(completedTableLSNsBytes), []byte("null")) {
			if err := json.Unmarshal(completedTableLSNsBytes, &req.CompletedTableLSNs); err != nil {
				return nil, fmt.Errorf("decoding completed table lsns: %w", err)
			}
		}

		snapshotRequests = append(snapshotRequests, req)
	}

	return snapshotRequests, nil
}

func (s *Store) GetSnapshotRequestsBySchema(ctx context.Context, schema string) ([]*snapshot.Request, error) {
	query := fmt.Sprintf(`SELECT schema_name,
	table_names,
	mode,
	start_lsn,
	end_lsn,
	COALESCE(completed_tables, '{}'::text[])::text AS completed_tables_raw,
	COALESCE(completed_table_lsns, '{}'::jsonb),
	status,
	errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
	rows, err := s.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("error getting snapshot requests: %w", err)
	}
	defer rows.Close()

	snapshotRequests := []*snapshot.Request{}
	for rows.Next() {
		req := &snapshot.Request{}
		var completedTables pq.StringArray
		var completedTablesRaw sql.NullString
		var completedTableLSNsBytes []byte
		var mode sql.NullString
		var startLSN sql.NullString
		var endLSN sql.NullString
		if err := rows.Scan(&req.Schema, &req.Tables, &mode, &startLSN, &endLSN, &completedTablesRaw, &completedTableLSNsBytes, &req.Status, &req.Errors); err != nil {
			return nil, fmt.Errorf("scanning snapshot request row: %w", err)
		}
		if mode.Valid {
			req.Mode = snapshot.Mode(mode.String)
		}
		if startLSN.Valid {
			req.StartLSN = startLSN.String
		}
		if endLSN.Valid {
			req.EndLSN = endLSN.String
		}
		if completedTablesRaw.Valid {
			if err := completedTables.Scan(completedTablesRaw.String); err != nil {
				return nil, fmt.Errorf("decoding completed tables: %w", err)
			}
		}
		req.CompletedTables = completedTables
		req.CompletedTableLSNs = make(map[string]string)
		if len(completedTableLSNsBytes) > 0 && !bytes.Equal(bytes.TrimSpace(completedTableLSNsBytes), []byte("null")) {
			if err := json.Unmarshal(completedTableLSNsBytes, &req.CompletedTableLSNs); err != nil {
				return nil, fmt.Errorf("decoding completed table lsns: %w", err)
			}
		}

		snapshotRequests = append(snapshotRequests, req)
	}

	return snapshotRequests, nil
}

func (s *Store) createTable(ctx context.Context) error {
	createSchemaQuery := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, store.SchemaName)
	_, err := s.conn.Exec(ctx, createSchemaQuery)
	if err != nil {
		return fmt.Errorf("error creating pgstream schema for snapshot store: %w", err)
	}

	createTableQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	req_id SERIAL PRIMARY KEY,
	schema_name TEXT,
	table_names TEXT[],
	mode TEXT NOT NULL DEFAULT 'full',
	start_lsn PG_LSN,
	end_lsn PG_LSN,
	completed_tables TEXT[] NOT NULL DEFAULT '{}',
	completed_table_lsns JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE,
	status TEXT CHECK (status IN ('requested', 'in progress', 'completed')),
	errors JSONB )`, snapshotsTable())
	_, err = s.conn.Exec(ctx, createTableQuery)
	if err != nil {
		return fmt.Errorf("error creating snapshots postgres table: %w", err)
	}

	if err := s.ensureSnapshotColumns(ctx); err != nil {
		return err
	}

	uniqueIndexQuery := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS schema_table_status_unique_index
	ON %s(schema_name,table_names) WHERE status != 'completed' AND mode != 'delta'`, snapshotsTable())
	_, err = s.conn.Exec(ctx, uniqueIndexQuery)
	if err != nil {
		return fmt.Errorf("error creating unique index on snapshots postgres table: %w", err)
	}

	return err
}

func (s *Store) ensureSnapshotColumns(ctx context.Context) error {
	queries := []struct {
		query string
		err   string
	}{
		{
			query: fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS completed_tables TEXT[] NOT NULL DEFAULT '{}'`, snapshotsTable()),
			err:   "ensuring completed_tables column",
		},
		{
			query: fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS completed_table_lsns JSONB NOT NULL DEFAULT '{}'::jsonb`, snapshotsTable()),
			err:   "ensuring completed_table_lsns column",
		},
		{
			query: fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'full'`, snapshotsTable()),
			err:   "ensuring mode column",
		},
		{
			query: fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS start_lsn PG_LSN`, snapshotsTable()),
			err:   "ensuring start_lsn column",
		},
		{
			query: fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS end_lsn PG_LSN`, snapshotsTable()),
			err:   "ensuring end_lsn column",
		},
	}

	for _, q := range queries {
		if _, err := s.conn.Exec(ctx, q.query); err != nil {
			return fmt.Errorf("%s: %w", q.err, err)
		}
	}
	return nil
}

func snapshotsTable() string {
	return postgres.QuoteQualifiedIdentifier(store.SchemaName, store.TableName)
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func nullString(val string) interface{} {
	if val == "" {
		return nil
	}
	return val
}

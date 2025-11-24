// SPDX-License-Identifier: Apache-2.0

package delta

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/wal"
	replicationpkg "github.com/xataio/pgstream/pkg/wal/replication"
	replicationpg "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func TestDeltaReaderStreamsWALChangesEndToEnd(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}
	tc.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var pgurl string
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "resolve caller path")
	confPath := filepath.Join(filepath.Dir(file), "..", "..", "stream", "integration", "config", "postgresql.conf")
	confPath = filepath.Clean(confPath)
	if _, err := os.Stat(confPath); err != nil {
		t.Fatalf("config file %s not accessible: %v", confPath, err)
	}
	cfg := testcontainers.ContainerConfig{
		Image:      testcontainers.Postgres14,
		ConfigFile: confPath,
		Env: map[string]string{
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
	}
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, cfg, &pgurl)
	require.NoError(t, err)
	defer cleanup()

	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer conn.Close(ctx)

	const (
		tableName       = "delta_reader_e2e"
		publicationName = "pgstream_delta_it_pub"
	)

	execSQL(t, ctx, conn, fmt.Sprintf(`CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT)`, tableName))
	execSQL(t, ctx, conn, fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, tableName))
	execSQL(t, ctx, conn, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE public.%s`, publicationName, tableName))
	logPublicationState(t, ctx, conn, publicationName)
	initalRows := execInsertReturningIDs(t, ctx, conn, fmt.Sprintf(`INSERT INTO %s(name) VALUES ('baseline') RETURNING id`, tableName))
	t.Logf("inserted baseline rows: %+v", initalRows)
	slotManager, err := NewSlotManager(SlotConfig{PostgresURL: pgurl})
	require.NoError(t, err)

	reader, err := NewReader(ReaderConfig{
		SlotManager: slotManager,
		PostgresURL: pgurl,
		Publication: publicationName,
	})
	require.NoError(t, err)

	startLSN := readCurrentLSN(t, ctx, conn)
	t.Logf("start LSN before snapshot delta: %s", startLSN)

	parser := replicationpg.NewLSNParser()
	start, err := parser.FromString(startLSN)
	require.NoError(t, err)

	decoder := NewPGOutputDecoder(pglib.NewMapper(conn), parser)

	slotName := fmt.Sprintf("pgstream_delta_it_%d", time.Now().UnixNano())
	logSlotState(t, ctx, conn, slotName)
	slotInfo, err := slotManager.CreateTemporarySlot(ctx, slotName)
	require.NoError(t, err)
	t.Logf("created slot %s with consistent_point=%s snapshot=%s", slotInfo.Name, slotInfo.ConsistentPoint, slotInfo.SnapshotName)
	defer slotManager.DropSlot(context.Background(), slotInfo.Name, false)
	logSlotState(t, ctx, conn, slotName)

	execSQL(t, ctx, conn, fmt.Sprintf(`INSERT INTO %s(name) VALUES ('delta_insert')`, tableName))
	execSQL(t, ctx, conn, fmt.Sprintf(`UPDATE %s SET name = 'delta_update' WHERE name = 'baseline'`, tableName))
	execSQL(t, ctx, conn, fmt.Sprintf(`DELETE FROM %s WHERE name = 'delta_insert'`, tableName))

	finalLSN := readCurrentLSN(t, ctx, conn)
	t.Logf("final target LSN after DML: %s", finalLSN)
	finalTarget, err := parser.FromString(finalLSN)
	require.NoError(t, err)
	require.Greater(t, finalTarget, start, "target LSN must be greater than start LSN")

	peeked := peekLogicalSlotChanges(t, ctx, conn, slotInfo.Name, publicationName)
	require.NotEmpty(t, peeked, "expected logical slot to have pending changes from DML")
	t.Logf("pg_logical_slot_peek_changes returned %d rows (first=%s)", len(peeked), peeked[0])

	var (
		events       []*wal.Event
		loggedEvents int
	)

	err = reader.StreamExistingSlotBetween(ctx, slotInfo, start, finalTarget, func(msg *pglib.ReplicationMessage) error {
		if msg != nil {
			t.Logf("received replication message LSN=%s replyRequested=%t", parser.ToString(replicationpkg.LSN(msg.LSN)), msg.ReplyRequested)
		}
		decoded, derr := decoder.Decode(ctx, msg)
		if derr != nil {
			return derr
		}
		if len(decoded) == 0 {
			return nil
		}
		events = append(events, decoded...)
		t.Logf("decoded %d events (total=%d)", len(decoded), len(events))
		if loggedEvents < 5 {
			for _, evt := range decoded {
				if evt == nil || evt.Data == nil {
					continue
				}
				t.Logf("wal event action=%s schema=%s table=%s lsn=%s timestamp=%s columns=%v", evt.Data.Action, evt.Data.Schema, evt.Data.Table, evt.Data.LSN, evt.Data.Timestamp, evt.Data.Columns)
				loggedEvents++
			}
		}
		return nil
	})
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(events), 3, "expected at least 3 WAL events")

	var insertEvent, updateEvent, deleteEvent *wal.Event
	for _, evt := range events {
		if evt == nil || evt.Data == nil {
			continue
		}
		if evt.Data.Schema != "public" || evt.Data.Table != tableName {
			continue
		}
		switch evt.Data.Action {
		case "I":
			if insertEvent == nil {
				insertEvent = evt
			}
		case "U":
			if updateEvent == nil {
				updateEvent = evt
			}
		case "D":
			if deleteEvent == nil {
				deleteEvent = evt
			}
		}
	}

	require.NotNil(t, insertEvent, "missing insert event")
	require.NotNil(t, updateEvent, "missing update event")
	require.NotNil(t, deleteEvent, "missing delete event")

	require.Equal(t, "public", insertEvent.Data.Schema)
	require.Equal(t, "I", insertEvent.Data.Action)
	require.Equal(t, "delta_insert", columnValue(t, insertEvent.Data.Columns, "name"))

	require.Equal(t, "U", updateEvent.Data.Action)
	require.Equal(t, "delta_update", columnValue(t, updateEvent.Data.Columns, "name"))
	require.Equal(t, "baseline", columnValue(t, updateEvent.Data.Identity, "name"))

	require.Equal(t, "D", deleteEvent.Data.Action)
	require.Equal(t, "delta_insert", columnValue(t, deleteEvent.Data.Identity, "name"))
}

func execSQL(t *testing.T, ctx context.Context, conn *pglib.Conn, query string) {
	t.Helper()
	_, err := conn.Exec(ctx, query)
	require.NoError(t, err)
}

func readCurrentLSN(t *testing.T, ctx context.Context, conn *pglib.Conn) string {
	t.Helper()
	var lsn string
	err := conn.QueryRow(ctx, []any{&lsn}, "SELECT pg_current_wal_lsn()")
	require.NoError(t, err)
	return lsn
}

func columnValue(t *testing.T, cols []wal.Column, name string) any {
	t.Helper()
	for _, col := range cols {
		if col.Name == name {
			return col.Value
		}
	}
	require.FailNowf(t, "column not found", "column %s not present in %+v", name, cols)
	return nil
}

func logPublicationState(t *testing.T, ctx context.Context, conn *pglib.Conn, publication string) {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1", publication)
	if err != nil {
		t.Logf("failed to list publication relations: %v", err)
		return
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var tbl string
		if err := rows.Scan(&tbl); err != nil {
			t.Logf("scan publication table: %v", err)
			return
		}
		tables = append(tables, tbl)
	}
	if err := rows.Err(); err != nil {
		t.Logf("publication rows error: %v", err)
	}
	t.Logf("publication %s tables=%v", publication, tables)
}

func execInsertReturningIDs(t *testing.T, ctx context.Context, conn *pglib.Conn, query string) []int64 {
	t.Helper()
	rows, err := conn.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

func logSlotState(t *testing.T, ctx context.Context, conn *pglib.Conn, slot string) {
	t.Helper()
	var restart, confirmed sql.NullString
	err := conn.QueryRow(ctx, []any{&restart, &confirmed}, "SELECT restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slot)
	if err != nil {
		t.Logf("slot %s state unavailable: %v", slot, err)
		return
	}
	restartLSN := "NULL"
	confirmedLSN := "NULL"
	if restart.Valid {
		restartLSN = restart.String
	}
	if confirmed.Valid {
		confirmedLSN = confirmed.String
	}
	t.Logf("slot %s state restart_lsn=%s confirmed_flush_lsn=%s", slot, restartLSN, confirmedLSN)
}

func peekLogicalSlotChanges(t *testing.T, ctx context.Context, conn *pglib.Conn, slotName, publication string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `
		SELECT (lsn)::text AS lsn_text, encode(data, 'hex') AS data_hex
		FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)`,
		slotName, publication)
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var lsn, dataHex string
		require.NoError(t, rows.Scan(&lsn, &dataHex))
		results = append(results, fmt.Sprintf("%s %s", lsn, dataHex))
	}
	require.NoError(t, rows.Err())
	return results
}

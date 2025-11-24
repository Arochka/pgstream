// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/snapshot"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	pgdumprestore "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	pgsnapshotstore "github.com/xataio/pgstream/pkg/snapshot/store/postgres"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func Test_PostgresDeltaBootstrap(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()
	confPath := integrationPostgresConfigPath()

	var sourceURL string
	sourceCleanup, err := testcontainers.SetupPostgresContainer(ctx, testcontainers.ContainerConfig{
		Image:      testcontainers.Postgres14,
		ConfigFile: confPath,
	}, &sourceURL)
	require.NoError(t, err)
	defer sourceCleanup()

	var storeURL string
	storeCleanup, err := testcontainers.SetupPostgresContainer(ctx, testcontainers.ContainerConfig{
		Image:      testcontainers.Postgres14,
		ConfigFile: confPath,
	}, &storeURL)
	require.NoError(t, err)
	defer storeCleanup()

	tableName := fmt.Sprintf("delta_bootstrap_%d", time.Now().UnixNano())
	execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf(`CREATE TABLE %s(id serial PRIMARY KEY, name text)`, tableName))
	execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf(`INSERT INTO %s(name) VALUES ('baseline_a'), ('baseline_b')`, tableName))

	initStream(t, ctx, sourceURL)

	publicationName := fmt.Sprintf("pgstream_delta_it_%d", time.Now().UnixNano())
	cfg := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: sourceURL,
				Replication: pgreplication.Config{
					PostgresURL: sourceURL,
				},
				SnapshotStoreURL: storeURL,
				Snapshot: &snapshotbuilder.SnapshotListenerConfig{
					Data: &pgsnapshotgenerator.Config{
						URL: sourceURL,
					},
					Adapter: adapter.SnapshotConfig{
						Tables: []string{fmt.Sprintf("public.%s", tableName)},
					},
					Schema: &snapshotbuilder.SchemaSnapshotConfig{
						DumpRestore: &pgdumprestore.Config{
							SourcePGURL: sourceURL,
							TargetPGURL: targetPGURL,
						},
					},
					Recorder: &snapshotbuilder.SnapshotRecorderConfig{
						SnapshotStoreURL: storeURL,
					},
					Delta: &snapshot.DeltaConfig{
						LagThresholdBytes: 1,
						PublicationName:   publicationName,
						PollInterval:      500 * time.Millisecond, // run lag detector within test timeout
						AutoPublication: &snapshot.AutoPublicationConfig{
							Enabled: true,
						},
					},
				},
			},
		},
		Processor: testPostgresProcessorCfg(sourceURL, withoutBulkIngestion),
	}

	targetConn, err := pglib.NewConn(ctx, targetPGURL)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	waitForRows := func(want int) bool {
		var count int
		err := targetConn.QueryRow(ctx, []any{&count}, fmt.Sprintf("SELECT count(*) FROM %s", tableName))
		if err != nil {
			return false
		}
		return count == want
	}

	streamCfg := cloneWithoutDelta(cfg)
	streamCtx, cancel := context.WithCancel(context.Background())
	runStream(t, streamCtx, streamCfg)

	requireCondition(t, func() bool {
		return waitForRows(2)
	})

	// backlog created after snapshot completed
	execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf(`INSERT INTO %s(name) VALUES ('delta_c'), ('delta_d')`, tableName))

	enqueueDeltaSnapshotRequests(t, ctx, cfg, storeURL)
	requireCondition(t, func() bool {
		return deltaRequestExists(ctx, storeURL, tableName)
	}, func() {
		dumpSnapshotRequests(t, ctx, storeURL)
	})

	// stop replication before running the delta snapshot replay
	cancel()
	time.Sleep(500 * time.Millisecond)

	deltaCfg := deltaOnlyConfig(cfg)
	require.NoError(t, stream.Snapshot(ctx, testLogger(), deltaCfg, nil))

	requireCondition(t, func() bool {
		return deltaRequestCompleted(ctx, storeURL, tableName)
	}, func() {
		dumpSnapshotRequests(t, ctx, storeURL)
	})
	requireCondition(t, func() bool {
		return waitForRows(4)
	}, func() {
		dumpTargetRows(t, ctx, targetConn, tableName)
	})
}

func deltaRequestCompleted(ctx context.Context, storeURL, table string) bool {
	conn, err := pglib.NewConn(ctx, storeURL)
	if err != nil {
		return false
	}
	defer conn.Close(ctx)

	var status string
	query := `SELECT status FROM pgstream.snapshot_requests WHERE mode = 'delta' AND $1 = ANY(table_names) ORDER BY req_id DESC LIMIT 1`
	if err := conn.QueryRow(ctx, []any{&status}, query, table); err != nil {
		return false
	}
	return status == string(snapshot.StatusCompleted)
}

func deltaRequestExists(ctx context.Context, storeURL, table string) bool {
	conn, err := pglib.NewConn(ctx, storeURL)
	if err != nil {
		return false
	}
	defer conn.Close(ctx)

	var exists bool
	query := `SELECT true FROM pgstream.snapshot_requests WHERE mode = 'delta' AND $1 = ANY(table_names) ORDER BY req_id DESC LIMIT 1`
	if err := conn.QueryRow(ctx, []any{&exists}, query, table); err != nil {
		return false
	}
	return exists
}

func dumpSnapshotRequests(t *testing.T, ctx context.Context, storeURL string) {
	conn, err := pglib.NewConn(ctx, storeURL)
	if err != nil {
		t.Logf("snapshot request dump skipped (store conn error: %v)", err)
		return
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `SELECT req_id, schema_name, table_names, mode, status, start_lsn, end_lsn, completed_tables FROM pgstream.snapshot_requests ORDER BY req_id`)
	if err != nil {
		t.Logf("snapshot request dump query error: %v", err)
		return
	}
	defer rows.Close()

	type entry struct {
		ID        int
		Schema    string
		Tables    []string
		Mode      string
		Status    string
		StartLSN  string
		EndLSN    string
		Completed []string
	}

	var buf []entry
	for rows.Next() {
		var e entry
		var tables []string
		var completed []string
		var start sql.NullString
		var end sql.NullString
		if err := rows.Scan(&e.ID, &e.Schema, &tables, &e.Mode, &e.Status, &start, &end, &completed); err != nil {
			t.Logf("snapshot request dump scan error: %v", err)
			return
		}
		if start.Valid {
			e.StartLSN = start.String
		}
		if end.Valid {
			e.EndLSN = end.String
		}
		e.Tables = append(e.Tables, tables...)
		e.Completed = append(e.Completed, completed...)
		buf = append(buf, e)
	}
	t.Logf("snapshot store requests: %#v", buf)
}

func dumpTargetRows(t *testing.T, ctx context.Context, conn pglib.Querier, table string) {
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", table))
	if err != nil {
		t.Logf("target dump query error: %v", err)
		return
	}
	defer rows.Close()
	type row struct {
		ID   int
		Name string
	}
	var entries []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ID, &r.Name); err != nil {
			t.Logf("target dump scan error: %v", err)
			return
		}
		entries = append(entries, r)
	}
	t.Logf("target rows: %#v", entries)
}

func enqueueDeltaSnapshotRequests(t *testing.T, ctx context.Context, cfg *stream.Config, storeURL string) {
	t.Helper()

	if cfg == nil || cfg.Listener.Postgres == nil || cfg.Listener.Postgres.Snapshot == nil {
		t.Fatal("snapshot configuration required for delta replay")
	}

	store, err := pgsnapshotstore.New(ctx, storeURL)
	require.NoError(t, err)
	defer store.Close()

	err = stream.PlanDeltaSnapshotRequests(ctx, testLogger(), cfg.Listener.Postgres.Snapshot, store, cfg.SourcePostgresURL())
	require.NoError(t, err)
}

func deltaOnlyConfig(cfg *stream.Config) *stream.Config {
	if cfg == nil || cfg.Listener.Postgres == nil || cfg.Listener.Postgres.Snapshot == nil {
		return cfg
	}

	clone := *cfg
	listener := *cfg.Listener.Postgres
	snapshotCfg := *listener.Snapshot
	snapshotCfg.Schema = nil
	listener.Snapshot = &snapshotCfg
	clone.Listener.Postgres = &listener
	return &clone
}

func cloneWithoutDelta(cfg *stream.Config) *stream.Config {
	if cfg == nil || cfg.Listener.Postgres == nil {
		return cfg
	}
	clone := *cfg
	listener := *cfg.Listener.Postgres
	if listener.Snapshot != nil {
		snapshotClone := *listener.Snapshot
		snapshotClone.Delta = nil
		listener.Snapshot = &snapshotClone
	}
	clone.Listener.Postgres = &listener
	return &clone
}

func requireCondition(t *testing.T, fn func() bool, onTimeout ...func()) {
	t.Helper()

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			for _, hook := range onTimeout {
				hook()
			}
			t.Fatal("condition not met before timeout")
		case <-ticker.C:
			if fn() {
				return
			}
		}
	}
}

// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	pgdumprestore "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func Test_SnapshotRecorderTracksCompletion(t *testing.T) {
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

	tableName := "snapshot_recorder_e2e_test"
	execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf(`CREATE TABLE %s (id serial PRIMARY KEY, name text)`, tableName))
	execQueryWithURL(t, ctx, sourceURL, fmt.Sprintf("INSERT INTO %s(name) VALUES ('alpha')", tableName))

	initStream(t, ctx, sourceURL)

	cfg := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: sourceURL,
				Replication: pgreplication.Config{
					PostgresURL: sourceURL,
				},
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
				},
			},
		},
		Processor: testPostgresProcessorCfg(sourceURL, withoutBulkIngestion),
	}

	require.NoError(t, stream.Snapshot(ctx, testLogger(), cfg, nil))

	storeConn, err := pglib.NewConn(ctx, storeURL)
	require.NoError(t, err)
	defer storeConn.Close(ctx)

	var completedTables pq.StringArray
	var completedTablesRaw sql.NullString
	var completedLSNs []byte
	require.NoError(t, storeConn.QueryRow(ctx, []any{&completedTablesRaw, &completedLSNs}, `SELECT COALESCE(completed_tables, '{}'::text[])::text, COALESCE(completed_table_lsns, '{}'::jsonb) FROM pgstream.snapshot_requests WHERE schema_name = $1 ORDER BY req_id DESC LIMIT 1`, "public"))
	require.True(t, completedTablesRaw.Valid)
	require.NoError(t, completedTables.Scan(completedTablesRaw.String))
	require.Contains(t, completedTables, tableName)

	var lsnMap map[string]string
	require.NoError(t, json.Unmarshal(completedLSNs, &lsnMap))
	lsn, ok := lsnMap[tableName]
	require.True(t, ok, "table missing from completed_table_lsns")
	require.NotEmpty(t, lsn)
}

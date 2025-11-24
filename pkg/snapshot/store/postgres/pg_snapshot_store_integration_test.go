// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/snapshot/store"
)

func TestStoreEnsuresSnapshotColumns(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgurl string
	pgCleanup, err := testcontainers.SetupPostgresContainer(ctx, testcontainers.ContainerConfig{
		Image: testcontainers.Postgres14,
	}, &pgurl)
	require.NoError(t, err)
	defer pgCleanup()

	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, store.SchemaName))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, snapshotsTable()))
	require.NoError(t, err)

	legacyTable := fmt.Sprintf(`CREATE TABLE %s(
	req_id SERIAL PRIMARY KEY,
	schema_name TEXT,
	table_names TEXT[],
	created_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE,
	status TEXT CHECK (status IN ('requested', 'in progress', 'completed')),
	errors JSONB )`, snapshotsTable())
	_, err = conn.Exec(ctx, legacyTable)
	require.NoError(t, err)

	snapshotStore, err := New(ctx, pgurl)
	require.NoError(t, err)
	defer snapshotStore.Close()

	rows, err := conn.Query(ctx, `
SELECT column_name, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2`, store.SchemaName, store.TableName)
	require.NoError(t, err)
	defer rows.Close()

	type columnInfo struct {
		isNullable   string
		columnResult sql.NullString
	}

	columns := map[string]columnInfo{}
	for rows.Next() {
		var name string
		var info columnInfo
		if err := rows.Scan(&name, &info.isNullable, &info.columnResult); err != nil {
			t.Fatal(err)
		}
		columns[name] = info
	}
	require.NoError(t, rows.Err())

	ctCol, ok := columns["completed_tables"]
	require.True(t, ok, "expected completed_tables column")
	require.Equal(t, "NO", ctCol.isNullable)
	require.True(t, ctCol.columnResult.Valid)
	require.True(t, strings.Contains(ctCol.columnResult.String, "{}"), "completed_tables default should initialize empty array")

	clsnsCol, ok := columns["completed_table_lsns"]
	require.True(t, ok, "expected completed_table_lsns column")
	require.Equal(t, "NO", clsnsCol.isNullable)
	require.True(t, clsnsCol.columnResult.Valid)
	require.True(t, strings.Contains(clsnsCol.columnResult.String, "{}"), "completed_table_lsns default should initialize empty json")
}

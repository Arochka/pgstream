// // SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	postgresmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/store"
)

const testCompletedTableLSN = "0/16B6A70"

func TestStore_CreateSnapshotRequest(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"test-table-1", "test-table-2"}
	testSnapshotRequest := &snapshot.Request{
		Schema: testSchema,
		Tables: testTables,
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier pglib.Querier

		wantErr error
	}{
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, call uint, s string, a ...any) (pglib.CommandTag, error) {
					switch call {
					case 1:
						wantCleanup := fmt.Sprintf(`DELETE FROM %s WHERE schema_name = $1 AND table_names = $2 AND status != 'completed'`, snapshotsTable())
						require.Equal(t, wantCleanup, s)
						require.Equal(t, []any{testSchema, pq.StringArray(testTables)}, a)
					case 2:
						wantQuery := fmt.Sprintf(`INSERT INTO %s (schema_name, table_names, mode, start_lsn, end_lsn, completed_tables, completed_table_lsns, created_at, updated_at, status)
	VALUES($1, $2, $3, $4, $5, $6, $7, now(), now(),'requested')`, snapshotsTable())
						require.Equal(t, wantQuery, s)
						wantAttr := []any{testSchema, pq.StringArray(testTables), snapshot.ModeFull, interface{}(nil), interface{}(nil), pq.StringArray{}, []byte("{}")}
						require.Equal(t, wantAttr, a)
					default:
						t.Fatalf("unexpected exec call %d", call)
					}
					return pglib.CommandTag{}, nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - delta request skips cleanup",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, call uint, s string, a ...any) (pglib.CommandTag, error) {
					switch call {
					case 1:
						wantQuery := fmt.Sprintf(`INSERT INTO %s (schema_name, table_names, mode, start_lsn, end_lsn, completed_tables, completed_table_lsns, created_at, updated_at, status)
	VALUES($1, $2, $3, $4, $5, $6, $7, now(), now(),'requested')`, snapshotsTable())
						require.Equal(t, wantQuery, s)
						wantAttr := []any{testSchema, pq.StringArray(testTables), snapshot.ModeDelta, interface{}(nil), interface{}(nil), pq.StringArray{}, []byte("{}")}
						require.Equal(t, wantAttr, a)
					default:
						t.Fatalf("unexpected exec call %d", call)
					}
					return pglib.CommandTag{}, nil
				},
			},

			wantErr: nil,
		},
		{
			name: "error - creating snapshot",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errTest
				},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			req := *testSnapshotRequest
			if tc.name == "ok - delta request skips cleanup" {
				req.Mode = snapshot.ModeDelta
			}
			err := store.CreateSnapshotRequest(context.Background(), &req)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_UpdateSnapshotRequest(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"test-table-1", "test-table-2"}
	testSnapshotRequest := snapshot.Request{
		Schema: testSchema,
		Tables: testTables,
		Status: snapshot.StatusInProgress,
	}

	errTest := errors.New("oh noes")
	testSchemaErr := snapshot.NewSchemaErrors(testSchema, errTest)

	tests := []struct {
		name    string
		querier pglib.Querier
		req     *snapshot.Request

		wantErr error
	}{
		{
			name: "ok - update without error",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, completed_tables = COALESCE($3, completed_tables), completed_table_lsns = COALESCE($4, completed_table_lsns), mode = COALESCE($5, mode), start_lsn = COALESCE($6, start_lsn), end_lsn = COALESCE($7, end_lsn), updated_at = now()
	WHERE schema_name = $8 AND table_names = $9 and status != 'completed'`,
						snapshotsTable())
					require.Equal(t, wantQuery, s)
					require.Equal(t, []any{snapshot.StatusInProgress, (*snapshot.SchemaErrors)(nil), interface{}(nil), interface{}(nil), snapshot.ModeFull, interface{}(nil), interface{}(nil), testSchema, pq.StringArray(testTables)}, a)
					return pglib.CommandTag{}, nil
				},
			},
			req: &testSnapshotRequest,

			wantErr: nil,
		},
		{
			name: "ok - update with error",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s SET status = $1, errors = $2, completed_tables = COALESCE($3, completed_tables), completed_table_lsns = COALESCE($4, completed_table_lsns), mode = COALESCE($5, mode), start_lsn = COALESCE($6, start_lsn), end_lsn = COALESCE($7, end_lsn), updated_at = now()
	WHERE schema_name = $8 AND table_names = $9 and status != 'completed'`,
						snapshotsTable())
					require.Equal(t, wantQuery, s)
					require.Equal(t, []any{snapshot.StatusInProgress, testSchemaErr, pq.StringArray{"table-completed"}, []byte(`{"table-completed":"0/16B6A70"}`), snapshot.ModeFull, interface{}(nil), interface{}(nil), testSchema, pq.StringArray(testTables)}, a)
					return pglib.CommandTag{}, nil
				},
			},
			req: func() *snapshot.Request {
				s := testSnapshotRequest
				s.Errors = testSchemaErr
				s.CompletedTables = []string{"table-completed"}
				s.CompletedTableLSNs = map[string]string{"table-completed": testCompletedTableLSN}
				return &s
			}(),

			wantErr: nil,
		},
		{
			name: "error - updating snapshot",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, s string, a ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errTest
				},
			},
			req: &testSnapshotRequest,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			err := store.UpdateSnapshotRequest(context.Background(), tc.req)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_GetSnapshotRequestsBySchema(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"test-table-1", "test-table-2"}
	errTest := errors.New("oh noes")
	testSnapshotRequest := snapshot.Request{
		Schema:             testSchema,
		Tables:             testTables,
		CompletedTables:    []string{"test-table-1"},
		CompletedTableLSNs: map[string]string{"test-table-1": testCompletedTableLSN},
		Status:             snapshot.StatusInProgress,
		Errors:             snapshot.NewSchemaErrors(testSchema, errTest),
		Mode:               snapshot.ModeFull,
	}

	tests := []struct {
		name    string
		querier pglib.Querier

		wantRequests []*snapshot.Request
		wantErr      error
	}{
		{
			name: "ok - no results",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,
	table_names,
	mode,
	start_lsn,
	end_lsn,
	COALESCE(completed_tables, '{}'::text[])::text AS completed_tables_raw,
	COALESCE(completed_table_lsns, '{}'::jsonb),
	status,
	errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSchema}, args)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(_ uint) bool { return false },
					}, nil
				},
			},

			wantRequests: []*snapshot.Request{},
			wantErr:      nil,
		},
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,
	table_names,
	mode,
	start_lsn,
	end_lsn,
	COALESCE(completed_tables, '{}'::text[])::text AS completed_tables_raw,
	COALESCE(completed_table_lsns, '{}'::jsonb),
	status,
	errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSchema}, args)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 9)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = testSchema

							tableNames, ok := dest[1].(*[]string)
							require.True(t, ok)
							*tableNames = testTables

							modeVal, ok := dest[2].(*sql.NullString)
							require.True(t, ok)
							*modeVal = sql.NullString{String: string(snapshot.ModeFull), Valid: true}

							start, ok := dest[3].(*sql.NullString)
							require.True(t, ok)
							*start = sql.NullString{}

							end, ok := dest[4].(*sql.NullString)
							require.True(t, ok)
							*end = sql.NullString{}

							completed, ok := dest[5].(*sql.NullString)
							require.True(t, ok)
							*completed = sql.NullString{String: "{test-table-1}", Valid: true}

							completedLSNs, ok := dest[6].(*[]byte)
							require.True(t, ok)
							*completedLSNs = []byte(`{"test-table-1":"0/16B6A70"}`)

							status, ok := dest[7].(*snapshot.Status)
							require.True(t, ok)
							*status = testSnapshotRequest.Status

							errs, ok := dest[8].(**snapshot.SchemaErrors)
							require.True(t, ok)
							*errs = testSnapshotRequest.Errors
							return nil
						},
					}, nil
				},
			},

			wantRequests: []*snapshot.Request{
				&testSnapshotRequest,
			},
			wantErr: nil,
		},
		{
			name: "error - querying",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
		{
			name: "error - scanning row",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,
	table_names,
	mode,
	start_lsn,
	end_lsn,
	COALESCE(completed_tables, '{}'::text[])::text AS completed_tables_raw,
	COALESCE(completed_table_lsns, '{}'::jsonb),
	status,
	errors FROM %s
	WHERE schema_name = $1 ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), queryLimit)
					require.Equal(t, []any{testSchema}, args)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return errTest
						},
					}, nil
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			requests, err := store.GetSnapshotRequestsBySchema(context.Background(), testSchema)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantRequests, requests)
		})
	}
}

func TestStore_GetSnapshotRequestsByStatus(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"test-table-1", "test-table-2"}
	errTest := errors.New("oh noes")
	testSnapshotRequest := snapshot.Request{
		Schema:             testSchema,
		Tables:             testTables,
		CompletedTables:    []string{"test-table-1"},
		CompletedTableLSNs: map[string]string{"test-table-1": testCompletedTableLSN},
		Status:             snapshot.StatusInProgress,
		Errors:             snapshot.NewSchemaErrors(testSchema, errTest),
		Mode:               snapshot.ModeFull,
	}

	tests := []struct {
		name    string
		querier pglib.Querier

		wantRequests []*snapshot.Request
		wantErr      error
	}{
		{
			name: "ok - no results",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,
	table_names,
	mode,
	start_lsn,
	end_lsn,
	COALESCE(completed_tables, '{}'::text[])::text AS completed_tables_raw,
	COALESCE(completed_table_lsns, '{}'::jsonb),
	status,
	errors FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), snapshot.StatusInProgress, queryLimit)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(_ uint) bool { return false },
					}, nil
				},
			},

			wantRequests: []*snapshot.Request{},
			wantErr:      nil,
		},
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,
	table_names,
	mode,
	start_lsn,
	end_lsn,
	COALESCE(completed_tables, '{}'::text[])::text AS completed_tables_raw,
	COALESCE(completed_table_lsns, '{}'::jsonb),
	status,
	errors FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), snapshot.StatusInProgress, queryLimit)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 9)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = testSchema

							tableNames, ok := dest[1].(*[]string)
							require.True(t, ok)
							*tableNames = testTables

							modeVal, ok := dest[2].(*sql.NullString)
							require.True(t, ok)
							*modeVal = sql.NullString{String: string(snapshot.ModeFull), Valid: true}

							start, ok := dest[3].(*sql.NullString)
							require.True(t, ok)
							*start = sql.NullString{}

							end, ok := dest[4].(*sql.NullString)
							require.True(t, ok)
							*end = sql.NullString{}

							completed, ok := dest[5].(*sql.NullString)
							require.True(t, ok)
							*completed = sql.NullString{String: "{test-table-1}", Valid: true}

							completedLSNs, ok := dest[6].(*[]byte)
							require.True(t, ok)
							*completedLSNs = []byte(`{"test-table-1":"0/16B6A70"}`)

							status, ok := dest[7].(*snapshot.Status)
							require.True(t, ok)
							*status = testSnapshotRequest.Status

							errs, ok := dest[8].(**snapshot.SchemaErrors)
							require.True(t, ok)
							*errs = testSnapshotRequest.Errors
							return nil
						},
					}, nil
				},
			},

			wantRequests: []*snapshot.Request{
				&testSnapshotRequest,
			},
			wantErr: nil,
		},
		{
			name: "error - querying",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
		{
			name: "error - scanning row",
			querier: &postgresmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					wantQuery := fmt.Sprintf(`SELECT schema_name,
	table_names,
	mode,
	start_lsn,
	end_lsn,
	COALESCE(completed_tables, '{}'::text[])::text AS completed_tables_raw,
	COALESCE(completed_table_lsns, '{}'::jsonb),
	status,
	errors FROM %s
	WHERE status = '%s' ORDER BY req_id ASC LIMIT %d`, snapshotsTable(), snapshot.StatusInProgress, queryLimit)
					require.Equal(t, wantQuery, query)
					return &postgresmocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return errTest
						},
					}, nil
				},
			},

			wantRequests: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{
				conn: tc.querier,
			}
			requests, err := store.GetSnapshotRequestsByStatus(context.Background(), snapshot.StatusInProgress)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantRequests, requests)
		})
	}
}

func TestStore_MarkTableCompleted(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTable := "test-table"
	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier pglib.Querier
		wantErr error
	}{
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
					wantQuery := fmt.Sprintf(`UPDATE %s
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
					require.Equal(t, wantQuery, query)
					require.Equal(t, []any{testTable, testSchema, testCompletedTableLSN}, args)
					return pglib.CommandTag{}, nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errTest
				},
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &Store{conn: tc.querier}
			err := store.MarkTableCompleted(context.Background(), testSchema, testTable, testCompletedTableLSN)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestStore_createTable(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		querier pglib.Querier

		wantErr error
	}{
		{
			name: "ok",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						wantQuery := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, store.SchemaName)
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 2:
						wantQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
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
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					case 3:
						require.Equal(t, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS completed_tables TEXT[] NOT NULL DEFAULT '{}'`, snapshotsTable()), s)
						require.Empty(t, a)
					case 4:
						require.Equal(t, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS completed_table_lsns JSONB NOT NULL DEFAULT '{}'::jsonb`, snapshotsTable()), s)
						require.Empty(t, a)
					case 5:
						require.Equal(t, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'full'`, snapshotsTable()), s)
						require.Empty(t, a)
					case 6:
						require.Equal(t, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS start_lsn PG_LSN`, snapshotsTable()), s)
						require.Empty(t, a)
					case 7:
						require.Equal(t, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS end_lsn PG_LSN`, snapshotsTable()), s)
						require.Empty(t, a)
					case 8:
						wantQuery := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS schema_table_status_unique_index
	ON %s(schema_name,table_names) WHERE status != 'completed' AND mode != 'delta'`, snapshotsTable())
						require.Equal(t, wantQuery, s)
						require.Empty(t, a)
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}

					return pglib.CommandTag{}, nil
				},
			},
			wantErr: nil,
		},
		{
			name: "error - creating schema",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - creating table",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						return pglib.CommandTag{}, nil
					case 2:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - ensuring completed_tables column",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						return pglib.CommandTag{}, nil
					case 2:
						return pglib.CommandTag{}, nil
					case 3:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - ensuring completed_table_lsns column",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						return pglib.CommandTag{}, nil
					case 2:
						return pglib.CommandTag{}, nil
					case 3:
						return pglib.CommandTag{}, nil
					case 4:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - ensuring mode column",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1, 2, 3, 4:
						return pglib.CommandTag{}, nil
					case 5:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - ensuring start_lsn column",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1, 2, 3, 4, 5:
						return pglib.CommandTag{}, nil
					case 6:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - ensuring end_lsn column",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1, 2, 3, 4, 5, 6:
						return pglib.CommandTag{}, nil
					case 7:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
		{
			name: "error - creating index",
			querier: &postgresmocks.Querier{
				ExecFn: func(ctx context.Context, i uint, s string, a ...any) (pglib.CommandTag, error) {
					switch i {
					case 1:
						return pglib.CommandTag{}, nil
					case 2:
						return pglib.CommandTag{}, nil
					case 3:
						return pglib.CommandTag{}, nil
					case 4:
						return pglib.CommandTag{}, nil
					case 5:
						return pglib.CommandTag{}, nil
					case 6:
						return pglib.CommandTag{}, nil
					case 7:
						return pglib.CommandTag{}, nil
					case 8:
						return pglib.CommandTag{}, errTest
					default:
						return pglib.CommandTag{}, fmt.Errorf("unexpected Exec call: %d", i)
					}
				},
			},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := Store{
				conn: tc.querier,
			}
			err := store.createTable(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

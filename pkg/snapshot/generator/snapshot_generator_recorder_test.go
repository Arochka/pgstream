// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotprogress "github.com/xataio/pgstream/pkg/snapshot/progress"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	snapshotstoremocks "github.com/xataio/pgstream/pkg/snapshot/store/mocks"
	replicationpg "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func TestSnapshotRecorder_CreateSnapshot(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"table1", "table2"}
	testSnapshot := snapshot.Snapshot{
		SchemaTables: map[string][]string{
			testSchema: testTables,
		},
	}

	newTestSnapshot := func() *snapshot.Snapshot {
		ss := testSnapshot
		return &ss
	}

	errTest := errors.New("oh noes")
	updateErr := errors.New("update error")

	tests := []struct {
		name      string
		store     snapshotstore.Store
		generator SnapshotGenerator
		assert    func(t *testing.T)

		wantErr error
	}{
		{
			name: "ok",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
						}, r)
						return nil
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot(), ss)
					return nil
				},
			},

			wantErr: nil,
		},
		func() struct {
			name      string
			store     snapshotstore.Store
			generator SnapshotGenerator
			assert    func(t *testing.T)
			wantErr   error
		} {
			var completionCalls []string
			store := &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					return nil
				},
				MarkTableCompletedFn: func(ctx context.Context, schema, table, lsn string) error {
					completionCalls = append(completionCalls, fmt.Sprintf("%s.%s", schema, table))
					return nil
				},
			}

			generator := &mockGenerator{}
			generator.createSnapshotFn = func(ctx context.Context, ss *snapshot.Snapshot) error {
				require.NotNil(t, generator.completionHook)
				for _, table := range ss.SchemaTables[testSchema] {
					generator.completionHook(ctx, testSchema, table, "0/16B6A70")
				}
				return nil
			}

			return struct {
				name      string
				store     snapshotstore.Store
				generator SnapshotGenerator
				assert    func(t *testing.T)
				wantErr   error
			}{
				name:      "ok - table completion recorded",
				store:     store,
				generator: generator,
				wantErr:   nil,
				assert: func(t *testing.T) {
					require.ElementsMatch(t, []string{"test-schema.table1", "test-schema.table2"}, completionCalls)
				},
			}
		}(),
		{
			name: "ok - all tables filtered out",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			generator: &mockGenerator{},

			wantErr: nil,
		},
		{
			name: "error - getting existing requests",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return nil, errTest
				},
			},
			generator: &mockGenerator{},

			wantErr: errTest,
		},
		{
			name: "error - snapshot error on wrapped generator",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
							Errors: snapshot.NewSchemaErrors(testSchema, errTest),
						}, r)
						return nil
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					return errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - recording snapshot request",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					return errTest
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					return errors.New("createSnapshotFn: should not be called")
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - updating snapshot request in progress",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return errTest
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{},

			wantErr: errTest,
		},
		{
			name: "error - updating snapshot request completed without errors",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
						}, r)
						return errTest
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot(), ss)
					return nil
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - updating snapshot request completed with snapshot and table errors",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
							Errors: &snapshot.SchemaErrors{
								Schema:       testSchema,
								GlobalErrors: []string{errTest.Error()},
								TableErrors: map[string]string{
									"table1": errTest.Error(),
								},
							},
						}, r)
						return updateErr
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot(), ss)
					return &snapshot.Errors{
						testSchema: &snapshot.SchemaErrors{
							Schema:       testSchema,
							GlobalErrors: []string{errTest.Error()},
							TableErrors: map[string]string{
								"table1": errTest.Error(),
							},
						},
					}
				},
			},

			wantErr: &snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{errTest.Error(), updateErr.Error()},
					TableErrors: map[string]string{
						"table1": errTest.Error(),
					},
				},
			},
		},
		{
			name: "error - updating snapshot request completed with table errors",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
				CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
					require.Equal(t, &snapshot.Request{
						Schema: testSchema,
						Tables: testTables,
					}, r)
					return nil
				},
				UpdateSnapshotRequestFn: func(ctx context.Context, i uint, r *snapshot.Request) error {
					switch i {
					case 1:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusInProgress,
						}, r)
						return nil
					case 2:
						require.Equal(t, &snapshot.Request{
							Schema: testSchema,
							Tables: testTables,
							Status: snapshot.StatusCompleted,
							Errors: &snapshot.SchemaErrors{
								Schema: testSchema,
								TableErrors: map[string]string{
									"table1": errTest.Error(),
								},
							},
						}, r)
						return updateErr
					default:
						return fmt.Errorf("unexpected call to UpdateSnapshotRequestFn: %d", i)

					}
				},
			},
			generator: &mockGenerator{
				createSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, newTestSnapshot(), ss)
					return &snapshot.Errors{
						testSchema: &snapshot.SchemaErrors{
							Schema: testSchema,
							TableErrors: map[string]string{
								"table1": errTest.Error(),
							},
						},
					}
				},
			},

			wantErr: &snapshot.Errors{
				testSchema: &snapshot.SchemaErrors{
					Schema:       testSchema,
					GlobalErrors: []string{updateErr.Error()},
					TableErrors: map[string]string{
						"table1": errTest.Error(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sr := NewSnapshotRecorder(tc.store, tc.generator, false, nil, nil)
			defer sr.Close()

			err := sr.CreateSnapshot(context.Background(), newTestSnapshot())
			if !errors.Is(err, tc.wantErr) {
				require.Equal(t, tc.wantErr, err)
			}
			if tc.assert != nil {
				tc.assert(t)
			}
		})
	}
}

func TestSnapshotRecorder_filterOutExistingSnapshots(t *testing.T) {
	t.Parallel()

	testSchema := "test-schema"
	testTables := []string{"table1", "table2"}
	errTest := errors.New("oh noes")

	tests := []struct {
		name                string
		store               snapshotstore.Store
		tables              []string
		repeatableSnapshots bool

		wantTables map[string][]string
		wantErr    error
	}{
		{
			name: "ok - partially completed schema skipped per table",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema:          testSchema,
							Tables:          testTables,
							CompletedTables: []string{"table1"},
							Status:          snapshot.StatusInProgress,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: {"table2"},
			},
		},
		{
			name: "ok - schema fully completed via progress",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema:          testSchema,
							Tables:          testTables,
							CompletedTables: testTables,
							Status:          snapshot.StatusInProgress,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{},
		},
		{
			name: "ok - no existing snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: testTables,
			},
		},
		{
			name: "ok - no existing snapshots with wildcard",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"table2"},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables: []string{"*"},
			wantTables: map[string][]string{
				testSchema: {"*"},
			},
		},
		{
			name: "ok - existing snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"table2"},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: {"table1"},
			},
		},
		{
			name: "ok - existing snapshots with repeatable snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return nil, errors.New("unexpected call to GetSnapshotRequestsBySchemaFn")
				},
			},
			repeatableSnapshots: true,
			wantTables: map[string][]string{
				testSchema: {"table1", "table2"},
			},
		},
		{
			name: "ok - existing wildcard snapshot with wildcard table",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"*"},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables:     []string{"*"},
			wantTables: map[string][]string{},
		},
		{
			name: "ok - existing wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"*"},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{},
		},
		{
			name: "ok - existing failed snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"table2"},
							Errors: snapshot.NewSchemaErrors(testSchema, errTest),
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: {"table1", "table2"},
			},
		},
		{
			name: "ok - existing failed wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"*"},
							Errors: &snapshot.SchemaErrors{
								Schema: testSchema,
								TableErrors: map[string]string{
									"table2": errTest.Error(),
								},
							},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables: []string{"*"},
			wantTables: map[string][]string{
				testSchema: {"table2"},
			},
		},
		{
			name: "ok - existing failed table on wildcard snapshot",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"*"},
							Errors: &snapshot.SchemaErrors{
								Schema: testSchema,
								TableErrors: map[string]string{
									"table2": errTest.Error(),
								},
							},
							Status: snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			wantTables: map[string][]string{
				testSchema: {"table2"},
			},
		},
		{
			name: "ok - ignores pending delta requests",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return []*snapshot.Request{
						{
							Schema: testSchema,
							Tables: []string{"table1"},
							Mode:   snapshot.ModeDelta,
							Status: snapshot.StatusRequested,
						},
						{
							Schema:          testSchema,
							Tables:          []string{"table1"},
							CompletedTables: []string{"table1"},
							Status:          snapshot.StatusCompleted,
						},
					}, nil
				},
			},
			tables:     []string{"table1"},
			wantTables: map[string][]string{},
		},
		{
			name: "error - retrieving existing snapshots",
			store: &snapshotstoremocks.Store{
				GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
					return nil, errTest
				},
			},
			wantTables: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tables := testTables
			if tc.tables != nil {
				tables = tc.tables
			}

			filteredTables, err := FilterPendingTables(
				context.Background(),
				tc.store,
				map[string][]string{testSchema: tables},
				tc.repeatableSnapshots,
			)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantTables, filteredTables)
		})
	}
}

func TestSnapshotRecorder_prepareDeltaRequests(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	makeStore := func(reqs []*snapshot.Request) *snapshotstoremocks.Store {
		return &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return nil, nil
			},
			GetSnapshotRequestsByStatusFn: func(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
				if status != snapshot.StatusRequested {
					return nil, nil
				}
				return reqs, nil
			},
		}
	}

	t.Run("respects max_tables_per_run", func(t *testing.T) {
		t.Parallel()

		reqs := []*snapshot.Request{
			{
				Schema:   "public",
				Tables:   []string{"foo"},
				Mode:     snapshot.ModeDelta,
				StartLSN: "0/1",
				EndLSN:   "0/10",
				Status:   snapshot.StatusRequested,
			},
			{
				Schema:   "public",
				Tables:   []string{"bar"},
				Mode:     snapshot.ModeDelta,
				StartLSN: "0/2",
				EndLSN:   "0/20",
				Status:   snapshot.StatusRequested,
			},
		}
		store := makeStore(reqs)
		recorder := NewSnapshotRecorder(store, &mockGenerator{}, false, nil, &snapshot.DeltaConfig{MaxTablesPerRun: 1})
		ss := &snapshot.Snapshot{SchemaTables: map[string][]string{}}

		require.NoError(t, recorder.prepareDeltaRequests(ctx, ss))
		require.Len(t, ss.DeltaRequests, 1)
		require.Equal(t, []string{"foo"}, ss.DeltaRequests[0].Tables)
	})

	t.Run("skips deltas below lag threshold", func(t *testing.T) {
		t.Parallel()

		reqs := []*snapshot.Request{
			{
				Schema:   "public",
				Tables:   []string{"foo"},
				Mode:     snapshot.ModeDelta,
				StartLSN: "0/10",
				EndLSN:   "0/14",
				Status:   snapshot.StatusRequested,
			},
		}
		store := makeStore(reqs)
		recorder := NewSnapshotRecorder(store, &mockGenerator{}, false, nil, &snapshot.DeltaConfig{
			LagThresholdBytes: 16,
		})
		ss := &snapshot.Snapshot{SchemaTables: map[string][]string{}}

		require.NoError(t, recorder.prepareDeltaRequests(ctx, ss))
		require.Len(t, ss.DeltaRequests, 0)
	})

	t.Run("skips temporary schemas", func(t *testing.T) {
		t.Parallel()

		reqs := []*snapshot.Request{
			{
				Schema:   "pg_temp_42",
				Tables:   []string{"mt_temp"},
				Mode:     snapshot.ModeDelta,
				StartLSN: "0/1",
				EndLSN:   "0/5",
				Status:   snapshot.StatusRequested,
			},
		}
		store := makeStore(reqs)
		recorder := NewSnapshotRecorder(store, &mockGenerator{}, false, nil, &snapshot.DeltaConfig{
			LagThresholdBytes: 1,
		})
		ss := &snapshot.Snapshot{SchemaTables: map[string][]string{}}

		require.NoError(t, recorder.prepareDeltaRequests(ctx, ss))
		require.Empty(t, ss.DeltaRequests)
	})
}

func TestSnapshotRecorder_CreateSnapshotCoordinatesDeltaProgress(t *testing.T) {
	t.Parallel()

	const lsnStr = "0/10"
	ctx := context.Background()
	parser := replicationpg.NewLSNParser()
	targetLSN, err := parser.FromString(lsnStr)
	require.NoError(t, err)

	newDeltaRequest := func() *snapshot.Request {
		return &snapshot.Request{
			Schema:   "public",
			Tables:   []string{"foo"},
			Mode:     snapshot.ModeDelta,
			StartLSN: "0/2",
			EndLSN:   lsnStr,
			Status:   snapshot.StatusRequested,
		}
	}

	newStore := func(req *snapshot.Request, completed *[]string) *snapshotstoremocks.Store {
		return &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return nil, nil
			},
			GetSnapshotRequestsByStatusFn: func(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
				switch status {
				case snapshot.StatusRequested:
					return []*snapshot.Request{req}, nil
				default:
					return nil, nil
				}
			},
			CreateSnapshotRequestFn: func(ctx context.Context, r *snapshot.Request) error {
				return fmt.Errorf("CreateSnapshotRequest should not be called for delta requests")
			},
			UpdateSnapshotRequestFn: func(ctx context.Context, _ uint, r *snapshot.Request) error {
				return nil
			},
			MarkTableCompletedFn: func(ctx context.Context, schema, table, lsn string) error {
				*completed = append(*completed, fmt.Sprintf("%s.%s:%s", schema, table, lsn))
				return nil
			},
		}
	}

	t.Run("pending skip promoted to completion on success", func(t *testing.T) {
		t.Parallel()

		req := newDeltaRequest()
		var completed []string
		store := newStore(req, &completed)
		progressTracker, err := snapshotprogress.New(ctx, store, parser, 0)
		require.NoError(t, err)

		gen := &mockGenerator{}
		gen.createSnapshotFn = func(ctx context.Context, ss *snapshot.Snapshot) error {
			require.Len(t, ss.DeltaRequests, 1)
			require.True(t, progressTracker.ShouldSkip("public", "foo", targetLSN))
			if gen.completionHook != nil {
				gen.completionHook(ctx, "public", "foo", lsnStr)
			}
			return nil
		}
		recorder := NewSnapshotRecorder(store, gen, false, progressTracker, &snapshot.DeltaConfig{MaxTablesPerRun: 1})

		err = recorder.CreateSnapshot(ctx, &snapshot.Snapshot{SchemaTables: map[string][]string{}})
		require.NoError(t, err)
		require.True(t, progressTracker.ShouldSkip("public", "foo", targetLSN))
		require.Equal(t, []string{"public.foo:0/10"}, completed)
	})

	t.Run("pending skip cleared on failure", func(t *testing.T) {
		t.Parallel()

		req := newDeltaRequest()
		var completed []string
		store := newStore(req, &completed)
		progressTracker, err := snapshotprogress.New(ctx, store, parser, 0)
		require.NoError(t, err)
		genErr := errors.New("boom")
		gen := &mockGenerator{}
		gen.createSnapshotFn = func(ctx context.Context, ss *snapshot.Snapshot) error {
			require.Len(t, ss.DeltaRequests, 1)
			require.True(t, progressTracker.ShouldSkip("public", "foo", targetLSN))
			return genErr
		}
		recorder := NewSnapshotRecorder(store, gen, false, progressTracker, &snapshot.DeltaConfig{MaxTablesPerRun: 1})

		err = recorder.CreateSnapshot(ctx, &snapshot.Snapshot{SchemaTables: map[string][]string{}})
		require.ErrorIs(t, err, genErr)
		require.False(t, progressTracker.ShouldSkip("public", "foo", targetLSN))
		require.Len(t, completed, 0)
	})

	t.Run("delta failure keeps request pending", func(t *testing.T) {
		t.Parallel()

		req := newDeltaRequest()
		storeStatus := []snapshot.Status{}
		var mu sync.Mutex
		store := &snapshotstoremocks.Store{
			GetSnapshotRequestsBySchemaFn: func(ctx context.Context, s string) ([]*snapshot.Request, error) {
				return nil, nil
			},
			GetSnapshotRequestsByStatusFn: func(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
				if status == snapshot.StatusRequested {
					return []*snapshot.Request{req}, nil
				}
				return nil, nil
			},
			CreateSnapshotRequestFn: func(context.Context, *snapshot.Request) error { return nil },
			UpdateSnapshotRequestFn: func(ctx context.Context, _ uint, r *snapshot.Request) error {
				mu.Lock()
				storeStatus = append(storeStatus, r.Status)
				mu.Unlock()
				return nil
			},
		}
		genErr := errors.New("delta boom")
		gen := &mockGenerator{}
		gen.createSnapshotFn = func(ctx context.Context, ss *snapshot.Snapshot) error {
			return genErr
		}
		recorder := NewSnapshotRecorder(store, gen, false, nil, &snapshot.DeltaConfig{MaxTablesPerRun: 1})

		err := recorder.CreateSnapshot(ctx, &snapshot.Snapshot{SchemaTables: map[string][]string{}})
		require.ErrorIs(t, err, genErr)
		require.GreaterOrEqual(t, len(storeStatus), 2)
		require.Equal(t, snapshot.StatusInProgress, storeStatus[0])
		require.Equal(t, snapshot.StatusRequested, storeStatus[len(storeStatus)-1])
		require.Equal(t, snapshot.StatusRequested, req.Status)
	})
}

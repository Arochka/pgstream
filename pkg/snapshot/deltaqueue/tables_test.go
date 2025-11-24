// SPDX-License-Identifier: Apache-2.0

package deltaqueue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
)

func TestExpandTablesSkipsTemporarySchemas(t *testing.T) {
	store := &fakeSnapshotStore{
		byStatus: map[snapshot.Status][]*snapshot.Request{
			snapshot.StatusCompleted: {
				{
					Schema:             "public",
					CompletedTableLSNs: map[string]string{"users": "0/1"},
					Mode:               snapshot.ModeFull,
				},
				{
					Schema:             "pg_temp_123",
					CompletedTableLSNs: map[string]string{"mt_temp": "0/2"},
					Mode:               snapshot.ModeFull,
				},
			},
		},
	}

	tables, err := ExpandTables(context.Background(), store, []Table{{Schema: "*", Name: "*"}})
	require.NoError(t, err)

	require.Equal(t, []Table{{Schema: "public", Name: "users"}}, tables)
}

type fakeSnapshotStore struct {
	byStatus map[snapshot.Status][]*snapshot.Request
}

func (f *fakeSnapshotStore) CreateSnapshotRequest(context.Context, *snapshot.Request) error {
	return nil
}
func (f *fakeSnapshotStore) UpdateSnapshotRequest(context.Context, *snapshot.Request) error {
	return nil
}

func (f *fakeSnapshotStore) GetSnapshotRequestsByStatus(ctx context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
	return f.byStatus[status], nil
}

func (f *fakeSnapshotStore) GetSnapshotRequestsBySchema(context.Context, string) ([]*snapshot.Request, error) {
	return nil, nil
}

func (f *fakeSnapshotStore) MarkTableCompleted(context.Context, string, string, string) error {
	return nil
}
func (f *fakeSnapshotStore) Close() error { return nil }

var _ snapshotstore.Store = (*fakeSnapshotStore)(nil)

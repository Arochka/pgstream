// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	snap "github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/deltaqueue"
)

func TestParseQualifiedTables_AllowsWildcards(t *testing.T) {
	tables, err := deltaqueue.ParseTablePatterns([]string{"public.users", "sales.*", "*.*"})
	require.NoError(t, err)
	require.Equal(t, []deltaqueue.Table{
		{Schema: "public", Name: "users"},
		{Schema: "sales", Name: "*"},
		{Schema: "*", Name: "*"},
	}, tables)
}

func TestExpandQualifiedTables_ResolvesWildcards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := &mockSnapshotStore{
		byStatus: map[snap.Status][]*snap.Request{
			snap.StatusCompleted: {
				{
					Schema:             "public",
					CompletedTables:    []string{"users"},
					CompletedTableLSNs: map[string]string{"users": "0/1"},
					Mode:               snap.ModeFull,
				},
			},
			snap.StatusInProgress: {
				{
					Schema:             "sales",
					CompletedTableLSNs: map[string]string{"orders": "0/2"},
					Mode:               snap.ModeFull,
				},
			},
		},
	}

	expanded, err := deltaqueue.ExpandTables(ctx, store, []deltaqueue.Table{{Schema: "*", Name: "*"}})
	require.NoError(t, err)
	require.ElementsMatch(t, []deltaqueue.Table{
		{Schema: "public", Name: "users"},
		{Schema: "sales", Name: "orders"},
	}, expanded)
}

type mockSnapshotStore struct {
	bySchema map[string][]*snap.Request
	byStatus map[snap.Status][]*snap.Request
}

func (m *mockSnapshotStore) CreateSnapshotRequest(context.Context, *snap.Request) error { return nil }
func (m *mockSnapshotStore) UpdateSnapshotRequest(context.Context, *snap.Request) error { return nil }
func (m *mockSnapshotStore) MarkTableCompleted(context.Context, string, string, string) error {
	return nil
}

func (m *mockSnapshotStore) GetSnapshotRequestsBySchema(ctx context.Context, schema string) ([]*snap.Request, error) {
	if m.bySchema == nil {
		return nil, nil
	}
	return m.bySchema[schema], nil
}

func (m *mockSnapshotStore) GetSnapshotRequestsByStatus(ctx context.Context, status snap.Status) ([]*snap.Request, error) {
	if m.byStatus == nil {
		return nil, nil
	}
	return m.byStatus[status], nil
}

func (m *mockSnapshotStore) Close() error { return nil }

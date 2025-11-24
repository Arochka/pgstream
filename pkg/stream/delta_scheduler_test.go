// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
)

func TestDeltaPlannerEnqueuesTablesAboveThreshold(t *testing.T) {
	store := newFakeDeltaStore()
	store.addSchemaRequests("public", []*snapshot.Request{
		{
			Schema:             "public",
			CompletedTableLSNs: map[string]string{"customers": "0/10"},
			Mode:               snapshot.ModeFull,
		},
	})

	cfg := &snapshotbuilder.SnapshotListenerConfig{
		Adapter: adapter.SnapshotConfig{
			Tables: []string{"public.customers"},
		},
		Delta: &snapshot.DeltaConfig{
			LagThresholdBytes: 8,
		},
	}

	planner := newDeltaPlanner(loglib.NewNoopLogger(), cfg, store, "postgres://source")
	require.NotNil(t, planner)
	planner.lsnReader = func(context.Context, string) (string, error) {
		return "0/20", nil
	}

	err := planner.Plan(context.Background())
	require.NoError(t, err)

	require.Len(t, store.created, 1)
	request := store.created[0]
	require.Equal(t, snapshot.ModeDelta, request.Mode)
	require.Equal(t, "public", request.Schema)
	require.Equal(t, []string{"customers"}, request.Tables)
	require.Equal(t, "0/10", request.StartLSN)
	require.Equal(t, "0/20", request.EndLSN)
}

func TestDeltaPlannerSkipsWhenLagBelowThreshold(t *testing.T) {
	store := newFakeDeltaStore()
	store.addSchemaRequests("public", []*snapshot.Request{
		{
			Schema:             "public",
			CompletedTableLSNs: map[string]string{"customers": "0/10"},
			Mode:               snapshot.ModeFull,
		},
	})

	cfg := &snapshotbuilder.SnapshotListenerConfig{
		Adapter: adapter.SnapshotConfig{
			Tables: []string{"public.customers"},
		},
		Delta: &snapshot.DeltaConfig{
			LagThresholdBytes: 32,
		},
	}
	planner := newDeltaPlanner(loglib.NewNoopLogger(), cfg, store, "postgres://source")
	require.NotNil(t, planner)
	planner.lsnReader = func(context.Context, string) (string, error) {
		return "0/18", nil
	}

	err := planner.Plan(context.Background())
	require.NoError(t, err)
	require.Empty(t, store.created)
}

func TestDeltaPlannerSkipsPendingTables(t *testing.T) {
	store := newFakeDeltaStore()
	store.addSchemaRequests("public", []*snapshot.Request{
		{
			Schema:             "public",
			CompletedTableLSNs: map[string]string{"customers": "0/2"},
			Mode:               snapshot.ModeFull,
		},
	})
	store.addStatusRequests(snapshot.StatusRequested, []*snapshot.Request{
		{
			Schema: "public",
			Tables: []string{"customers"},
			Mode:   snapshot.ModeDelta,
		},
	})

	cfg := &snapshotbuilder.SnapshotListenerConfig{
		Adapter: adapter.SnapshotConfig{
			Tables: []string{"public.customers"},
		},
		Delta: &snapshot.DeltaConfig{
			LagThresholdBytes: 1,
		},
	}
	planner := newDeltaPlanner(loglib.NewNoopLogger(), cfg, store, "postgres://source")
	require.NotNil(t, planner)
	planner.lsnReader = func(context.Context, string) (string, error) {
		return "0/20", nil
	}

	err := planner.Plan(context.Background())
	require.NoError(t, err)
	require.Empty(t, store.created)
}

func TestDeltaPlannerSkipsTemporarySchemas(t *testing.T) {
	store := newFakeDeltaStore()
	store.addSchemaRequests("pg_temp_123", []*snapshot.Request{
		{
			Schema:             "pg_temp_123",
			CompletedTableLSNs: map[string]string{"mt_temp": "0/5"},
			Mode:               snapshot.ModeFull,
		},
	})

	cfg := &snapshotbuilder.SnapshotListenerConfig{
		Adapter: adapter.SnapshotConfig{
			Tables: []string{"pg_temp_123.mt_temp"},
		},
		Delta: &snapshot.DeltaConfig{
			LagThresholdBytes: 1,
		},
	}
	planner := newDeltaPlanner(loglib.NewNoopLogger(), cfg, store, "postgres://source")
	require.NotNil(t, planner)
	planner.lsnReader = func(context.Context, string) (string, error) {
		return "0/10", nil
	}

	err := planner.Plan(context.Background())
	require.NoError(t, err)
	require.Empty(t, store.created)
}

type fakeDeltaStore struct {
	mu       sync.Mutex
	bySchema map[string][]*snapshot.Request
	byStatus map[snapshot.Status][]*snapshot.Request
	created  []*snapshot.Request
}

func newFakeDeltaStore() *fakeDeltaStore {
	return &fakeDeltaStore{
		bySchema: make(map[string][]*snapshot.Request),
		byStatus: make(map[snapshot.Status][]*snapshot.Request),
	}
}

func (f *fakeDeltaStore) addSchemaRequests(schema string, reqs []*snapshot.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bySchema[schema] = append(f.bySchema[schema], reqs...)
}

func (f *fakeDeltaStore) addStatusRequests(status snapshot.Status, reqs []*snapshot.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byStatus[status] = append(f.byStatus[status], reqs...)
}

func (f *fakeDeltaStore) CreateSnapshotRequest(_ context.Context, req *snapshot.Request) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, req)
	f.byStatus[snapshot.StatusRequested] = append(f.byStatus[snapshot.StatusRequested], req)
	return nil
}

func (f *fakeDeltaStore) UpdateSnapshotRequest(context.Context, *snapshot.Request) error {
	return nil
}

func (f *fakeDeltaStore) GetSnapshotRequestsByStatus(_ context.Context, status snapshot.Status) ([]*snapshot.Request, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	requests := f.byStatus[status]
	if len(requests) == 0 {
		return nil, nil
	}
	out := make([]*snapshot.Request, len(requests))
	copy(out, requests)
	return out, nil
}

func (f *fakeDeltaStore) GetSnapshotRequestsBySchema(_ context.Context, schema string) ([]*snapshot.Request, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	requests := f.bySchema[schema]
	if len(requests) == 0 {
		return nil, nil
	}
	out := make([]*snapshot.Request, len(requests))
	copy(out, requests)
	return out, nil
}

func (f *fakeDeltaStore) MarkTableCompleted(context.Context, string, string, string) error {
	return nil
}

func (f *fakeDeltaStore) Close() error { return nil }

var _ snapshotstore.Store = (*fakeDeltaStore)(nil)

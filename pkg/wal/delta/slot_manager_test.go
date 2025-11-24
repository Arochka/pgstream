// SPDX-License-Identifier: Apache-2.0

package delta

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	postgresmocks "github.com/xataio/pgstream/internal/postgres/mocks"
)

func TestSlotManager_CreateTemporarySlot(t *testing.T) {
	t.Parallel()

	const slotName = "tmp_slot"
	ctx := context.Background()

	mgr, err := NewSlotManager(SlotConfig{PostgresURL: "postgres://localhost/db"},
		WithReplicationConnBuilder(func(context.Context) (pglib.ReplicationQuerier, error) {
			return &postgresmocks.ReplicationConn{
				CreateReplicationSlotFn: func(ctx context.Context, name, plugin string, opts pglib.CreateReplicationSlotOptions) (pglib.CreateReplicationSlotResult, error) {
					require.Equal(t, slotName, name)
					require.Equal(t, defaultLogicalPlugin, plugin)
					require.False(t, opts.Temporary)
					require.Equal(t, pglib.SnapshotActionExport, opts.SnapshotAction)
					return pglib.CreateReplicationSlotResult{
						SlotName:        name,
						SnapshotName:    "snap",
						ConsistentPoint: "0/16",
					}, nil
				},
				CloseFn: func(context.Context) error { return nil },
			}, nil
		}))
	require.NoError(t, err)

	info, err := mgr.CreateTemporarySlot(ctx, slotName)
	require.NoError(t, err)
	require.Equal(t, "snap", info.SnapshotName)
	require.Equal(t, "0/16", info.ConsistentPoint)
}

func TestSlotManager_DropSlot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dropCalled := false

	mgr, err := NewSlotManager(SlotConfig{PostgresURL: "postgres://localhost/db"},
		WithReplicationConnBuilder(func(context.Context) (pglib.ReplicationQuerier, error) {
			return &postgresmocks.ReplicationConn{
				DropReplicationSlotFn: func(ctx context.Context, slot string, wait bool) error {
					dropCalled = true
					require.Equal(t, "tmp_slot", slot)
					require.True(t, wait)
					return nil
				},
				CloseFn: func(context.Context) error { return nil },
			}, nil
		}))
	require.NoError(t, err)

	err = mgr.DropSlot(ctx, "tmp_slot", true)
	require.NoError(t, err)
	require.True(t, dropCalled)
}

func TestSlotManager_ConnErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := errors.New("boom")

	mgr, err := NewSlotManager(SlotConfig{PostgresURL: "postgres://localhost/db"},
		WithReplicationConnBuilder(func(context.Context) (pglib.ReplicationQuerier, error) {
			return nil, wantErr
		}))
	require.NoError(t, err)

	_, err = mgr.CreateTemporarySlot(ctx, "tmp")
	require.ErrorIs(t, err, wantErr)

	err = mgr.DropSlot(ctx, "tmp", false)
	require.ErrorIs(t, err, wantErr)
}

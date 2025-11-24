// SPDX-License-Identifier: Apache-2.0

package delta

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	postgresmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	replication "github.com/xataio/pgstream/pkg/wal/replication"
	replicationpg "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func TestReader_StreamBetween(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	publication := "pgstream_delta_test"
	slotMgr, err := NewSlotManager(SlotConfig{PostgresURL: "postgres://localhost/db"},
		WithReplicationConnBuilder(func(ctx context.Context) (pglib.ReplicationQuerier, error) {
			return &postgresmocks.ReplicationConn{
				CreateReplicationSlotFn: func(ctx context.Context, slotName, plugin string, opts pglib.CreateReplicationSlotOptions) (pglib.CreateReplicationSlotResult, error) {
					return pglib.CreateReplicationSlotResult{
						SlotName:        slotName,
						SnapshotName:    "snap",
						ConsistentPoint: "0/1",
					}, nil
				},
				DropReplicationSlotFn: func(ctx context.Context, slotName string, wait bool) error {
					return nil
				},
				CloseFn: func(context.Context) error { return nil },
			}, nil
		}))
	require.NoError(t, err)

	var messages []uint64
	var standbyUpdates []uint64

	reader, err := NewReader(ReaderConfig{
		SlotManager:     slotMgr,
		PostgresURL:     "postgres://localhost/db",
		Publication:     publication,
		ProtocolVersion: 1,
	}, WithReaderConnBuilder(func(ctx context.Context) (pglib.ReplicationQuerier, error) {
		return &postgresmocks.ReplicationConn{
			StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error {
				require.Equal(t, uint64(mustParseLSN(t, "0/1")), cfg.StartPos)
				require.Equal(t, []string{
					`"proto_version" '1'`,
					fmt.Sprintf(`"publication_names" '%s'`, publication),
				}, cfg.PluginArguments)
				return nil
			},
			ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
				if len(messages) == 0 {
					val := uint64(mustParseLSN(t, "0/2"))
					messages = append(messages, val)
					return &pglib.ReplicationMessage{LSN: val}, nil
				}
				if len(messages) == 1 {
					val := uint64(mustParseLSN(t, "0/4"))
					messages = append(messages, val)
					return &pglib.ReplicationMessage{LSN: val}, nil
				}
				return &pglib.ReplicationMessage{
					LSN:            uint64(mustParseLSN(t, "0/5")),
					ReplyRequested: true,
				}, nil
			},
			SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
				standbyUpdates = append(standbyUpdates, lsn)
				return nil
			},
			CloseFn: func(context.Context) error { return nil },
		}, nil
	}))
	require.NoError(t, err)

	err = reader.StreamBetween(ctx, "delta_slot", mustParseLSN(t, "0/1"), mustParseLSN(t, "0/5"), func(msg *pglib.ReplicationMessage) error {
		return nil
	})
	require.NoError(t, err)
	require.Len(t, messages, 2)
}

func TestReader_StreamBetweenErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := errors.New("boom")
	publication := "pgstream_delta_test"
	slotMgr, err := NewSlotManager(SlotConfig{PostgresURL: "postgres://localhost/db"},
		WithReplicationConnBuilder(func(context.Context) (pglib.ReplicationQuerier, error) {
			return &postgresmocks.ReplicationConn{
				CreateReplicationSlotFn: func(ctx context.Context, slotName, plugin string, opts pglib.CreateReplicationSlotOptions) (pglib.CreateReplicationSlotResult, error) {
					return pglib.CreateReplicationSlotResult{
						SlotName:        slotName,
						SnapshotName:    "snap",
						ConsistentPoint: "0/1",
					}, nil
				},
				DropReplicationSlotFn: func(ctx context.Context, slotName string, wait bool) error {
					return nil
				},
				CloseFn: func(context.Context) error { return nil },
			}, nil
		}))
	require.NoError(t, err)

	reader, err := NewReader(ReaderConfig{
		SlotManager:     slotMgr,
		PostgresURL:     "postgres://localhost/db",
		Publication:     publication,
		ProtocolVersion: 1,
	}, WithReaderConnBuilder(func(ctx context.Context) (pglib.ReplicationQuerier, error) {
		return &postgresmocks.ReplicationConn{
			StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error {
				return nil
			},
			ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
				return nil, wantErr
			},
			SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
				return nil
			},
			CloseFn: func(context.Context) error { return nil },
		}, nil
	}))
	require.NoError(t, err)

	err = reader.StreamBetween(ctx, "delta_slot", mustParseLSN(t, "0/1"), mustParseLSN(t, "0/2"), func(msg *pglib.ReplicationMessage) error {
		return nil
	})
	require.ErrorIs(t, err, wantErr)
}

func TestReader_StreamBetweenAllowsStartBeforeConsistent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	publication := "pgstream_delta_test"
	slotMgr, err := NewSlotManager(SlotConfig{PostgresURL: "postgres://localhost/db"},
		WithReplicationConnBuilder(func(ctx context.Context) (pglib.ReplicationQuerier, error) {
			return &postgresmocks.ReplicationConn{
				CreateReplicationSlotFn: func(ctx context.Context, slotName, plugin string, opts pglib.CreateReplicationSlotOptions) (pglib.CreateReplicationSlotResult, error) {
					return pglib.CreateReplicationSlotResult{
						SlotName:        slotName,
						SnapshotName:    "snap",
						ConsistentPoint: "0/4",
					}, nil
				},
				DropReplicationSlotFn: func(ctx context.Context, slotName string, wait bool) error { return nil },
				CloseFn:               func(context.Context) error { return nil },
			}, nil
		}))
	require.NoError(t, err)

	reader, err := NewReader(ReaderConfig{
		SlotManager:     slotMgr,
		PostgresURL:     "postgres://localhost/db",
		Publication:     publication,
		ProtocolVersion: 1,
	}, WithReaderConnBuilder(func(ctx context.Context) (pglib.ReplicationQuerier, error) {
		return &postgresmocks.ReplicationConn{
			StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error {
				require.Equal(t, uint64(mustParseLSN(t, "0/2")), cfg.StartPos)
				return nil
			},
			ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
				return &pglib.ReplicationMessage{LSN: uint64(mustParseLSN(t, "0/3"))}, nil
			},
			SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error { return nil },
			CloseFn:                   func(context.Context) error { return nil },
		}, nil
	}))
	require.NoError(t, err)

	err = reader.StreamBetween(ctx, "delta_slot", mustParseLSN(t, "0/2"), mustParseLSN(t, "0/3"), func(msg *pglib.ReplicationMessage) error {
		return nil
	})
	require.NoError(t, err)
}

func TestReader_StreamExistingSlotBetween(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	publication := "pgstream_delta_test"
	slotMgr, err := NewSlotManager(SlotConfig{PostgresURL: "postgres://localhost/db"},
		WithReplicationConnBuilder(func(context.Context) (pglib.ReplicationQuerier, error) {
			return &postgresmocks.ReplicationConn{
				CloseFn: func(context.Context) error { return nil },
			}, nil
		}))
	require.NoError(t, err)

	reader, err := NewReader(ReaderConfig{
		SlotManager:     slotMgr,
		PostgresURL:     "postgres://localhost/db",
		Publication:     publication,
		ProtocolVersion: 1,
	}, WithReaderConnBuilder(func(ctx context.Context) (pglib.ReplicationQuerier, error) {
		return &postgresmocks.ReplicationConn{
			StartReplicationFn: func(ctx context.Context, cfg pglib.ReplicationConfig) error {
				require.Equal(t, uint64(mustParseLSN(t, "0/1")), cfg.StartPos)
				return nil
			},
			ReceiveMessageFn: func(ctx context.Context) (*pglib.ReplicationMessage, error) {
				return &pglib.ReplicationMessage{LSN: uint64(mustParseLSN(t, "0/3"))}, nil
			},
			SendStandbyStatusUpdateFn: func(ctx context.Context, lsn uint64) error {
				return nil
			},
			CloseFn: func(context.Context) error { return nil },
		}, nil
	}))
	require.NoError(t, err)

	err = reader.StreamExistingSlotBetween(ctx, SlotInfo{
		Name:            "delta_slot",
		ConsistentPoint: "0/2",
	}, mustParseLSN(t, "0/1"), mustParseLSN(t, "0/3"), func(msg *pglib.ReplicationMessage) error {
		return nil
	})
	require.NoError(t, err)
}

func mustParseLSN(t testing.TB, value string) replication.LSN {
	t.Helper()
	parser := replicationpg.NewLSNParser()
	lsn, err := parser.FromString(value)
	require.NoError(t, err)
	return lsn
}

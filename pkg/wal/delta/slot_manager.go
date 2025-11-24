// SPDX-License-Identifier: Apache-2.0

package delta

import (
	"context"
	"errors"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
)

const (
	defaultLogicalPlugin = "pgoutput"
)

// SlotConfig contains the information required to manage logical replication slots.
type SlotConfig struct {
	PostgresURL string
	Plugin      string
}

// SlotInfo represents the state returned when creating a logical slot.
type SlotInfo struct {
	Name            string
	SnapshotName    string
	ConsistentPoint string
}

// SlotManager handles lifecycle actions for logical replication slots dedicated
// to incremental snapshots.
type SlotManager struct {
	connBuilder func(context.Context) (pglib.ReplicationQuerier, error)
	plugin      string
	logger      loglib.Logger
}

type SlotOption func(*SlotManager)

// NewSlotManager builds a slot manager that can create or drop logical slots.
func NewSlotManager(cfg SlotConfig, opts ...SlotOption) (*SlotManager, error) {
	if cfg.PostgresURL == "" {
		return nil, errors.New("postgres url is required")
	}
	plugin := cfg.Plugin
	if plugin == "" {
		plugin = defaultLogicalPlugin
	}

	manager := &SlotManager{
		connBuilder: func(ctx context.Context) (pglib.ReplicationQuerier, error) {
			return pglib.NewReplicationConn(ctx, cfg.PostgresURL)
		},
		plugin: plugin,
		logger: loglib.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(manager)
	}

	return manager, nil
}

// WithLogger overrides the default logger used by the slot manager.
func WithLogger(logger loglib.Logger) SlotOption {
	return func(m *SlotManager) {
		m.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "delta_slot_manager",
		})
	}
}

// WithReplicationConnBuilder allows injecting a custom replication connection builder (useful for tests).
func WithReplicationConnBuilder(builder func(context.Context) (pglib.ReplicationQuerier, error)) SlotOption {
	return func(m *SlotManager) {
		m.connBuilder = builder
	}
}

// CreateTemporarySlot creates a logical slot dedicated to delta replay. Even though the
// slot is dropped after the run, it is not marked TEMPORARY so it can be used from any
// subsequent replication connection.
func (m *SlotManager) CreateTemporarySlot(ctx context.Context, slotName string) (SlotInfo, error) {
	conn, err := m.connBuilder(ctx)
	if err != nil {
		return SlotInfo{}, err
	}
	defer conn.Close(ctx)

	m.logger.Info("creating delta slot", loglib.Fields{"slot": slotName})
	result, err := conn.CreateReplicationSlot(ctx, slotName, m.plugin, pglib.CreateReplicationSlotOptions{
		Temporary:      false,
		SnapshotAction: pglib.SnapshotActionExport,
	})
	if err != nil {
		return SlotInfo{}, fmt.Errorf("create replication slot: %w", err)
	}

	return SlotInfo{
		Name:            result.SlotName,
		SnapshotName:    result.SnapshotName,
		ConsistentPoint: result.ConsistentPoint,
	}, nil
}

// DropSlot removes the logical slot if it exists.
func (m *SlotManager) DropSlot(ctx context.Context, slotName string, wait bool) error {
	conn, err := m.connBuilder(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	m.logger.Info("dropping delta slot", loglib.Fields{"slot": slotName})
	if err := conn.DropReplicationSlot(ctx, slotName, wait); err != nil {
		return fmt.Errorf("drop replication slot: %w", err)
	}
	return nil
}

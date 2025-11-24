// SPDX-License-Identifier: Apache-2.0

package delta

import (
	"context"
	"fmt"
	"time"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationpg "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

// Reader streams WAL data between start and end LSN using a temporary logical slot.
type Reader struct {
	slotManager *SlotManager
	connBuilder func(context.Context) (pglib.ReplicationQuerier, error)
	logger      loglib.Logger
	pluginArgs  []string
}

type ReaderConfig struct {
	SlotManager *SlotManager
	PostgresURL string
	Publication string
	// ProtocolVersion controls the pgoutput protocol version passed to the plugin.
	// Defaults to 1 when unset.
	ProtocolVersion int
}

type ReaderOption func(*Reader)

func NewReader(cfg ReaderConfig, opts ...ReaderOption) (*Reader, error) {
	if cfg.SlotManager == nil {
		return nil, fmt.Errorf("slot manager required")
	}
	if cfg.PostgresURL == "" {
		return nil, fmt.Errorf("postgres url required")
	}
	if cfg.Publication == "" {
		return nil, fmt.Errorf("pgoutput publication required")
	}
	protoVersion := cfg.ProtocolVersion
	if protoVersion <= 0 {
		protoVersion = 1
	}
	r := &Reader{
		slotManager: cfg.SlotManager,
		connBuilder: func(ctx context.Context) (pglib.ReplicationQuerier, error) {
			return pglib.NewReplicationConn(ctx, cfg.PostgresURL)
		},
		logger:     loglib.NewNoopLogger(),
		pluginArgs: buildPluginArgs(cfg.Publication, protoVersion),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

func WithReaderLogger(l loglib.Logger) ReaderOption {
	return func(r *Reader) {
		r.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "delta_wal_reader",
		})
	}
}

func WithReaderConnBuilder(builder func(context.Context) (pglib.ReplicationQuerier, error)) ReaderOption {
	return func(r *Reader) {
		r.connBuilder = builder
	}
}

// StreamBetween creates a temporary slot and streams WAL messages from startLSN up to endLSN (inclusive),
// calling the handler for each message.
func (r *Reader) StreamBetween(ctx context.Context, slotName string, startLSN, endLSN replication.LSN, handler func(*pglib.ReplicationMessage) error) error {
	slotInfo, err := r.slotManager.CreateTemporarySlot(ctx, slotName)
	if err != nil {
		return err
	}
	defer r.slotManager.DropSlot(context.Background(), slotInfo.Name, false)

	return r.streamWithSlot(ctx, slotInfo, startLSN, endLSN, handler)
}

// StreamExistingSlotBetween reuses an already created logical slot to stream WAL messages between the provided
// LSNs. The caller is responsible for dropping the slot afterwards.
func (r *Reader) StreamExistingSlotBetween(ctx context.Context, slotInfo SlotInfo, startLSN, endLSN replication.LSN, handler func(*pglib.ReplicationMessage) error) error {
	if slotInfo.Name == "" {
		return fmt.Errorf("slot name required")
	}
	return r.streamWithSlot(ctx, slotInfo, startLSN, endLSN, handler)
}

func (r *Reader) streamWithSlot(ctx context.Context, slotInfo SlotInfo, startLSN, endLSN replication.LSN, handler func(*pglib.ReplicationMessage) error) error {
	if handler == nil {
		return fmt.Errorf("handler required")
	}

	conn, err := r.connBuilder(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	parser := replicationpg.NewLSNParser()
	startPos := startLSN
	var consistent replication.LSN
	if slotInfo.ConsistentPoint != "" {
		var err error
		consistent, err = parser.FromString(slotInfo.ConsistentPoint)
		if err != nil {
			return fmt.Errorf("parse consistent point: %w", err)
		}
		if startPos == 0 {
			startPos = consistent
		} else if startPos < consistent {
			r.logger.Debug("delta reader start LSN precedes slot consistent point", loglib.Fields{
				"slot":       slotInfo.Name,
				"start_lsn":  parser.ToString(startPos),
				"consistent": slotInfo.ConsistentPoint,
			})
		}
	}
	if startPos == 0 {
		startPos = consistent
	}

	if err := conn.StartReplication(ctx, pglib.ReplicationConfig{
		SlotName:        slotInfo.Name,
		StartPos:        uint64(startPos),
		PluginArguments: r.pluginArgs,
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	target := uint64(endLSN)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receive wal: %w", err)
		}
		if msg == nil {
			continue
		}
		if err := handler(msg); err != nil {
			return err
		}
		select {
		case <-ticker.C:
			if err := conn.SendStandbyStatusUpdate(ctx, msg.LSN); err != nil {
				return fmt.Errorf("send standby status update: %w", err)
			}
		default:
		}
		if msg.LSN >= target {
			r.logger.Info("delta reader reached target LSN", loglib.Fields{
				"lsn": parser.ToString(replication.LSN(msg.LSN)),
			})
			break
		}
	}
	return nil
}

func buildPluginArgs(publication string, protoVersion int) []string {
	return []string{
		fmt.Sprintf(`"proto_version" '%d'`, protoVersion),
		fmt.Sprintf(`"publication_names" '%s'`, publication),
	}
}

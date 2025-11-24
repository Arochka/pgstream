// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	snapshotprogress "github.com/xataio/pgstream/pkg/snapshot/progress"
	"github.com/xataio/pgstream/pkg/wal/checkpointer"
	snapshotadapter "github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
)

func runPendingDeltaSnapshots(
	ctx context.Context,
	logger loglib.Logger,
	cfg *Config,
	checkpoint checkpointer.Checkpoint,
	instrumentation *otel.Instrumentation,
	progress *snapshotprogress.Progress,
) error {
	deltaSnapshotCfg := deltaOnlySnapshotConfig(cfg.Listener.Postgres.Snapshot)
	if deltaSnapshotCfg == nil {
		return fmt.Errorf("delta snapshot requires data snapshot configuration")
	}

	snapshotProcessor, snapshotCloser, err := newProcessor(ctx, logger, cfg, checkpoint, processorTypeSnapshot, instrumentation, nil, nil)
	if err != nil {
		return err
	}
	defer snapshotCloser()

	snapshotGenerator, err := snapshotbuilder.NewSnapshotGenerator(ctx, deltaSnapshotCfg, snapshotProcessor, logger, instrumentation, progress)
	if err != nil {
		return err
	}
	defer snapshotGenerator.Close()

	return snapshotGenerator.CreateSnapshot(ctx)
}

func deltaOnlySnapshotConfig(base *snapshotbuilder.SnapshotListenerConfig) *snapshotbuilder.SnapshotListenerConfig {
	if base == nil {
		return nil
	}
	if base.Data == nil {
		return nil
	}

	clone := *base
	clone.Adapter = snapshotadapter.SnapshotConfig{}
	clone.Schema = nil

	return &clone
}

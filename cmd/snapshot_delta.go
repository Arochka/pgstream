// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"errors"
	"fmt"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot/deltaqueue"
	pgsnapshotstore "github.com/xataio/pgstream/pkg/snapshot/store/postgres"
	"github.com/xataio/pgstream/pkg/stream"
)

func enqueueDeltaSnapshotRequests(ctx context.Context, cfg *stream.Config, logger loglib.Logger) error {
	if cfg == nil || cfg.Listener.Postgres == nil || cfg.Listener.Postgres.Snapshot == nil {
		return errors.New("delta snapshots require a postgres snapshot configuration")
	}
	storeURL := cfg.SnapshotStoreURL()
	if storeURL == "" {
		return errors.New("delta snapshots require a snapshot recorder store")
	}

	store, err := pgsnapshotstore.New(ctx, storeURL)
	if err != nil {
		return fmt.Errorf("creating snapshot recorder store: %w", err)
	}
	defer store.Close()

	if err := stream.PlanDeltaSnapshotRequests(ctx, logger, cfg.Listener.Postgres.Snapshot, store, cfg.SourcePostgresURL()); err != nil {
		if errors.Is(err, deltaqueue.ErrNoCompletedTables) {
			logger.Info("running full snapshot to seed delta backlog")
			if snapErr := stream.Snapshot(ctx, logger, cfg, nil); snapErr != nil {
				return fmt.Errorf("running full snapshot before delta: %w", snapErr)
			}
			if err := stream.PlanDeltaSnapshotRequests(ctx, logger, cfg.Listener.Postgres.Snapshot, store, cfg.SourcePostgresURL()); err != nil {
				return fmt.Errorf("planning delta snapshot requests after full snapshot: %w", err)
			}
			return nil
		}
		return fmt.Errorf("planning delta snapshot requests: %w", err)
	}

	return nil
}

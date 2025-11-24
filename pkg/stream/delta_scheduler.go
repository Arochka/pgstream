// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"fmt"
	"strings"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/deltaqueue"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	replication "github.com/xataio/pgstream/pkg/wal/replication"
	replicationpg "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

// PlanDeltaSnapshotRequests enqueues delta snapshot requests once using the
// configured lag threshold. It replaces the old polling scheduler so that
// delta catch-up runs before replication starts.
func PlanDeltaSnapshotRequests(ctx context.Context, logger loglib.Logger, cfg *snapshotbuilder.SnapshotListenerConfig, store snapshotstore.Store, sourceURL string) error {
	planner := newDeltaPlanner(logger, cfg, store, sourceURL)
	if planner == nil {
		return nil
	}
	return planner.Plan(ctx)
}

type deltaPlanner struct {
	logger    loglib.Logger
	store     snapshotstore.Store
	cfg       *snapshotbuilder.SnapshotListenerConfig
	sourceURL string
	parser    replication.LSNParser
	lsnReader func(context.Context, string) (string, error)
}

func newDeltaPlanner(logger loglib.Logger, cfg *snapshotbuilder.SnapshotListenerConfig, store snapshotstore.Store, sourceURL string) *deltaPlanner {
	if cfg == nil || cfg.Delta == nil || store == nil || sourceURL == "" {
		return nil
	}
	if len(cfg.Adapter.Tables) == 0 {
		return nil
	}
	logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
		loglib.ModuleField: "delta_scheduler",
	})
	return &deltaPlanner{
		logger:    logger,
		store:     store,
		cfg:       cfg,
		sourceURL: sourceURL,
		parser:    replicationpg.NewLSNParser(),
		lsnReader: readCurrentLSN,
	}
}

func (p *deltaPlanner) Plan(ctx context.Context) error {
	if p == nil || p.cfg == nil || p.cfg.Delta == nil || p.cfg.Delta.LagThresholdBytes == 0 {
		return nil
	}

	patterns, err := deltaqueue.ParseTablePatterns(p.cfg.Adapter.Tables)
	if err != nil {
		return err
	}
	tables, err := deltaqueue.ExpandTables(ctx, p.store, patterns)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}

	targetLSNStr, err := p.lsnReader(ctx, p.sourceURL)
	if err != nil {
		return err
	}
	targetLSN, err := p.parser.FromString(targetLSNStr)
	if err != nil {
		return fmt.Errorf("parse current WAL LSN: %w", err)
	}

	pending := p.pendingTableSet(ctx)
	lagThreshold := p.cfg.Delta.LagThresholdBytes
	maxTables := p.cfg.Delta.MaxTablesPerRun
	scheduled := 0

	for _, table := range tables {
		if isTemporarySchema(table.Schema) {
			continue
		}
		key := tableKey(table)
		if _, exists := pending[key]; exists {
			continue
		}
		startLSN, err := deltaqueue.LatestCompletedTableLSN(ctx, p.store, p.parser, table.Schema, table.Name)
		if err != nil {
			continue
		}
		startValue, err := p.parser.FromString(startLSN)
		if err != nil {
			continue
		}
		var diff uint64
		if targetLSN > startValue {
			diff = uint64(targetLSN - startValue)
		}
		if diff < lagThreshold {
			continue
		}

		req := &snapshot.Request{
			Schema:   table.Schema,
			Tables:   []string{table.Name},
			Mode:     snapshot.ModeDelta,
			StartLSN: startLSN,
			EndLSN:   targetLSNStr,
		}
		if err := p.store.CreateSnapshotRequest(ctx, req); err != nil {
			p.logger.Warn(err, "creating delta snapshot request", loglib.Fields{
				"schema": table.Schema,
				"table":  table.Name,
			})
			continue
		}
		p.logger.Info("auto-enqueued delta snapshot", loglib.Fields{
			"schema":     table.Schema,
			"table":      table.Name,
			"start_lsn":  startLSN,
			"target_lsn": targetLSNStr,
			"lag_bytes":  diff,
		})
		pending[key] = struct{}{}
		scheduled++
		if maxTables > 0 && scheduled >= maxTables {
			break
		}
	}
	return nil
}

func (p *deltaPlanner) pendingTableSet(ctx context.Context) map[string]struct{} {
	set := make(map[string]struct{})
	statuses := []snapshot.Status{snapshot.StatusRequested, snapshot.StatusInProgress}
	for _, status := range statuses {
		reqs, err := p.store.GetSnapshotRequestsByStatus(ctx, status)
		if err != nil {
			continue
		}
		for _, req := range reqs {
			if req == nil || req.Schema == "" || req.Mode != snapshot.ModeDelta {
				continue
			}
			if isTemporarySchema(req.Schema) {
				continue
			}
			for _, table := range req.Tables {
				if table == "" {
					continue
				}
				set[req.Schema+"."+table] = struct{}{}
			}
		}
	}
	return set
}

func tableKey(tbl deltaqueue.Table) string {
	return tbl.Schema + "." + tbl.Name
}

func readCurrentLSN(ctx context.Context, url string) (string, error) {
	conn, err := pglib.NewConn(ctx, url)
	if err != nil {
		return "", fmt.Errorf("creating postgres connection: %w", err)
	}
	defer conn.Close(ctx)

	var lsn string
	if err := conn.QueryRow(ctx, []any{&lsn}, "SELECT pg_current_wal_lsn()"); err != nil {
		return "", fmt.Errorf("reading current WAL LSN: %w", err)
	}
	return lsn, nil
}

func isTemporarySchema(schema string) bool {
	return strings.HasPrefix(schema, "pg_temp_")
}

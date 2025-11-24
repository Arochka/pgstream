// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/progress"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	"github.com/xataio/pgstream/pkg/wal/replication"
	replicationpg "github.com/xataio/pgstream/pkg/wal/replication/postgres"
	"golang.org/x/sync/errgroup"
)

// SnapshotRecorder is a decorator around a snapshot generator that will record
// the snapshot request status.
type SnapshotRecorder struct {
	wrapped             SnapshotGenerator
	store               snapshotstore.Store
	repeatableSnapshots bool
	progress            *progress.Progress
	deltaConfig         *snapshot.DeltaConfig
	tableCompletionMu   sync.Mutex
	tableCompletionErr  error
	userTableHook       TableCompletionHook
	lsnParser           replication.LSNParser
	plannedDeltaTables  []schemaTable
}

const updateTimeout = time.Minute

type schemaTable struct {
	schema string
	table  string
}

func isTemporarySchema(schema string) bool {
	return strings.HasPrefix(schema, "pg_temp_")
}

// NewSnapshotRecorder wraps the generator with a recorder that persists request
// status and coordinates snapshot progress tracking.
func NewSnapshotRecorder(store snapshotstore.Store, generator SnapshotGenerator, repeatableSnapshots bool, progressTracker *progress.Progress, deltaCfg *snapshot.DeltaConfig) *SnapshotRecorder {
	sr := &SnapshotRecorder{
		wrapped:             generator,
		store:               store,
		repeatableSnapshots: repeatableSnapshots,
		progress:            progressTracker,
		deltaConfig:         deltaCfg,
		lsnParser:           replicationpg.NewLSNParser(),
	}
	if notifier, ok := generator.(TableCompletionNotifier); ok {
		notifier.SetTableCompletionHook(sr.tableCompleted)
	}
	return sr
}

func (s *SnapshotRecorder) tableCompleted(ctx context.Context, schema, table, lsn string) {
	s.handleTableCompleted(ctx, schema, table, lsn)
	s.tableCompletionMu.Lock()
	userHook := s.userTableHook
	s.tableCompletionMu.Unlock()
	if userHook != nil {
		userHook(ctx, schema, table, lsn)
	}
}

func (s *SnapshotRecorder) CreateSnapshot(ctx context.Context, ss *snapshot.Snapshot) error {
	if err := s.prepareDeltaRequests(ctx, ss); err != nil {
		return err
	}
	defer s.cleanupPendingDelta()

	var err error
	ss.SchemaTables, err = FilterPendingTables(ctx, s.store, ss.SchemaTables, s.repeatableSnapshots)
	if err != nil {
		return err
	}

	// no tables to snapshot
	if !ss.HasTables() && len(ss.DeltaRequests) == 0 {
		return nil
	}

	requests := s.createRequests(ss)

	if err := s.markSnapshotInProgress(ctx, requests); err != nil {
		return err
	}

	err = s.wrapped.CreateSnapshot(ctx, ss)
	if hookErr := s.tableCompletionError(); hookErr != nil {
		err = errors.Join(err, hookErr)
	}

	return s.markSnapshotCompleted(ctx, requests, err)
}

func (s *SnapshotRecorder) Close() error {
	s.store.Close()
	return s.wrapped.Close()
}

func (s *SnapshotRecorder) createRequests(ss *snapshot.Snapshot) []*snapshot.Request {
	total := len(ss.SchemaTables) + len(ss.DeltaRequests)
	requests := make([]*snapshot.Request, 0, total)
	for schema, tables := range ss.SchemaTables {
		req := &snapshot.Request{
			Schema: schema,
			Tables: tables,
		}
		requests = append(requests, req)
	}
	if len(ss.DeltaRequests) > 0 {
		requests = append(requests, ss.DeltaRequests...)
	}
	return requests
}

func (s *SnapshotRecorder) prepareDeltaRequests(ctx context.Context, ss *snapshot.Snapshot) error {
	if s.deltaConfig != nil && s.store != nil && !ss.HasTables() {
		planned, err := s.planDeltaRequests(ctx)
		if err != nil {
			return err
		}
		if len(planned) > 0 {
			ss.DeltaRequests = append(ss.DeltaRequests, planned...)
		}
	}

	if len(ss.DeltaRequests) > 0 {
		s.registerPendingDelta(ss.DeltaRequests)
	} else {
		s.plannedDeltaTables = nil
	}
	return nil
}

func (s *SnapshotRecorder) planDeltaRequests(ctx context.Context) ([]*snapshot.Request, error) {
	if s.store == nil {
		return nil, nil
	}
	reqs, err := s.store.GetSnapshotRequestsByStatus(ctx, snapshot.StatusRequested)
	if err != nil {
		return nil, fmt.Errorf("loading pending delta snapshot requests: %w", err)
	}

	if len(reqs) == 0 {
		return nil, nil
	}

	planned := make([]*snapshot.Request, 0, len(reqs))
	var scheduledTables int
	for _, req := range reqs {
		if req == nil || req.Mode != snapshot.ModeDelta {
			continue
		}
		if isTemporarySchema(req.Schema) {
			continue
		}
		if req.StartLSN == "" || len(req.Tables) == 0 {
			continue
		}
		if s.deltaConfig.LagThresholdBytes > 0 && !s.passesLagThreshold(req) {
			continue
		}

		if s.deltaConfig.MaxTablesPerRun > 0 {
			remaining := s.deltaConfig.MaxTablesPerRun - scheduledTables
			if remaining <= 0 {
				break
			}
			if len(req.Tables) > remaining && len(planned) > 0 {
				continue
			}
		}

		planned = append(planned, req)
		scheduledTables += len(req.Tables)

		if s.deltaConfig.MaxTablesPerRun > 0 && scheduledTables >= s.deltaConfig.MaxTablesPerRun {
			break
		}
	}

	return planned, nil
}

func (s *SnapshotRecorder) passesLagThreshold(req *snapshot.Request) bool {
	if s.deltaConfig == nil || s.deltaConfig.LagThresholdBytes == 0 {
		return true
	}
	if req.EndLSN == "" || req.StartLSN == "" {
		return true
	}

	start, err := s.lsnParser.FromString(req.StartLSN)
	if err != nil {
		return false
	}
	end, err := s.lsnParser.FromString(req.EndLSN)
	if err != nil {
		return false
	}

	if end <= start {
		return false
	}

	return uint64(end-start) >= s.deltaConfig.LagThresholdBytes
}

func (s *SnapshotRecorder) registerPendingDelta(requests []*snapshot.Request) {
	if s.progress == nil || len(requests) == 0 {
		return
	}

	s.plannedDeltaTables = s.plannedDeltaTables[:0]
	for _, req := range requests {
		if req == nil || req.Mode != snapshot.ModeDelta {
			continue
		}
		target := req.EndLSN
		if target == "" {
			target = req.StartLSN
		}
		if target == "" {
			continue
		}
		lsn, err := s.lsnParser.FromString(target)
		if err != nil {
			continue
		}
		for _, table := range req.Tables {
			s.progress.AddPendingDelta(req.Schema, table, lsn)
			s.plannedDeltaTables = append(s.plannedDeltaTables, schemaTable{schema: req.Schema, table: table})
		}
	}
}

func (s *SnapshotRecorder) cleanupPendingDelta() {
	if s.progress == nil || len(s.plannedDeltaTables) == 0 {
		return
	}
	for _, entry := range s.plannedDeltaTables {
		s.progress.ClearPendingDelta(entry.schema, entry.table)
	}
	s.plannedDeltaTables = nil
}

func (s *SnapshotRecorder) markSnapshotInProgress(ctx context.Context, requests []*snapshot.Request) error {
	eg, ctx := errgroup.WithContext(ctx)
	// create one request per schema
	for _, req := range requests {
		r := req
		eg.Go(func() error {
			if !s.isExistingRequest(r) {
				if err := s.store.CreateSnapshotRequest(ctx, r); err != nil {
					return err
				}
			}
			// the snapshot will start immediately
			r.MarkInProgress()
			return s.store.UpdateSnapshotRequest(ctx, r)
		})
	}

	return eg.Wait()
}

func (s *SnapshotRecorder) isExistingRequest(req *snapshot.Request) bool {
	return req != nil && req.Status != ""
}

func (s *SnapshotRecorder) markSnapshotCompleted(ctx context.Context, requests []*snapshot.Request, err error) error {
	// make sure we can update the request status in the store regardless of
	// context cancelations
	if ctx.Err() != nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, updateTimeout)
	defer cancel()

	getSchemaErrors := func(schema string, err error) *snapshot.SchemaErrors {
		if err == nil {
			return nil
		}
		snapshotErr := &snapshot.Errors{}
		if errors.As(err, &snapshotErr) {
			return snapshotErr.GetSchemaErrors(schema)
		}
		return snapshot.NewSchemaErrors(schema, err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	for _, req := range requests {
		eg.Go(func() error {
			schemaErrs := getSchemaErrors(req.Schema, err)
			if err == nil || req.Mode != snapshot.ModeDelta {
				req.MarkCompleted(req.Schema, schemaErrs)
			} else {
				req.Status = snapshot.StatusRequested
				if schemaErrs != nil {
					req.Errors = schemaErrs
				} else {
					req.Errors = snapshot.NewSchemaErrors(req.Schema, err)
				}
				req.CompletedTables = nil
				req.CompletedTableLSNs = nil
			}
			if err == nil && req.Mode == snapshot.ModeDelta {
				if len(req.CompletedTables) == 0 {
					req.CompletedTables = append(req.CompletedTables, req.Tables...)
				}
				if req.CompletedTableLSNs == nil {
					req.CompletedTableLSNs = make(map[string]string, len(req.Tables))
				}
				targetLSN := req.EndLSN
				if targetLSN == "" {
					targetLSN = req.StartLSN
				}
				for _, table := range req.Tables {
					if table == "" {
						continue
					}
					req.CompletedTableLSNs[table] = targetLSN
				}
			}
			if updateErr := s.store.UpdateSnapshotRequest(ctx, req); updateErr != nil {
				if err == nil {
					return updateErr
				}
				if schemaErrs == nil {
					schemaErrs = snapshot.NewSchemaErrors(req.Schema, updateErr)
				} else {
					schemaErrs.AddGlobalError(updateErr)
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return err
}

func (s *SnapshotRecorder) handleTableCompleted(ctx context.Context, schema, table, lsn string) {
	if err := s.store.MarkTableCompleted(ctx, schema, table, lsn); err != nil {
		s.setTableCompletionError(fmt.Errorf("recording completion for %s.%s: %w", schema, table, err))
		return
	}
	if s.progress != nil {
		s.progress.RecordSnapshotCompletion(schema, table, lsn)
	}
}

func (s *SnapshotRecorder) setTableCompletionError(err error) {
	if err == nil {
		return
	}
	s.tableCompletionMu.Lock()
	defer s.tableCompletionMu.Unlock()
	if s.tableCompletionErr == nil {
		s.tableCompletionErr = err
	} else {
		s.tableCompletionErr = errors.Join(s.tableCompletionErr, err)
	}
}

func (s *SnapshotRecorder) tableCompletionError() error {
	s.tableCompletionMu.Lock()
	defer s.tableCompletionMu.Unlock()
	return s.tableCompletionErr
}

func (s *SnapshotRecorder) SetTableCompletionHook(hook TableCompletionHook) {
	s.tableCompletionMu.Lock()
	s.userTableHook = hook
	s.tableCompletionMu.Unlock()
	if notifier, ok := s.wrapped.(TableCompletionNotifier); ok {
		notifier.SetTableCompletionHook(s.tableCompleted)
	}
}

// FilterPendingTables removes from the provided schema tables map any tables
// that have already been snapshotted successfully and recorded in the snapshot
// store, unless repeatable snapshots are enabled.
func FilterPendingTables(ctx context.Context, snapshotStore snapshotstore.Store, schemaTables map[string][]string, repeatableSnapshots bool) (map[string][]string, error) {
	if repeatableSnapshots {
		return schemaTables, nil
	}

	filteredSchemaTables := make(map[string][]string, len(schemaTables))
	for schema, tables := range schemaTables {
		filteredTables, err := filterPendingSchemaTables(ctx, snapshotStore, schema, tables)
		if err != nil {
			return nil, fmt.Errorf("filtering existing snapshots for schema %s: %w", schema, err)
		}
		if len(filteredTables) > 0 {
			filteredSchemaTables[schema] = filteredTables
		}
	}
	return filteredSchemaTables, nil
}

func filterPendingSchemaTables(ctx context.Context, snapshotStore snapshotstore.Store, schema string, tables []string) ([]string, error) {
	snapshotRequests, err := snapshotStore.GetSnapshotRequestsBySchema(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("retrieving existing snapshots for schema: %w", err)
	}

	if len(snapshotRequests) == 0 {
		return tables, nil
	}

	existingRequests := map[string]*snapshot.Request{}
	completedTables := map[string]struct{}{}
	for _, req := range snapshotRequests {
		if req.Mode == snapshot.ModeDelta {
			continue
		}
		for _, table := range req.Tables {
			existingRequests[table] = req
		}
		for _, table := range req.CompletedTables {
			completedTables[table] = struct{}{}
		}
	}

	const wildcard = "*"
	wildCardReq, wildcardFound := existingRequests[wildcard]
	filteredTables := make([]string, 0, len(tables))
	for _, table := range tables {
		if table == wildcard {
			if wildcardFound && wildCardReq.Status == snapshot.StatusCompleted {
				if wildCardReq.Errors == nil {
					continue
				}
				if !wildCardReq.Errors.IsGlobalError() {
					filteredTables = append(filteredTables, wildCardReq.Errors.GetFailedTables()...)
					continue
				}
			}
			filteredTables = append(filteredTables, table)
			continue
		}

		if _, completed := completedTables[table]; completed {
			continue
		}

		tableReq, tableFound := existingRequests[table]
		if tableFound && tableReq.Status == snapshot.StatusCompleted && (tableReq.Errors == nil || !tableReq.HasFailedForTable(table)) {
			continue
		}

		if !tableFound && wildcardFound && wildCardReq.Status == snapshot.StatusCompleted {
			if wildCardReq.Errors == nil || !wildCardReq.HasFailedForTable(table) {
				continue
			}
			filteredTables = append(filteredTables, table)
			continue
		}

		filteredTables = append(filteredTables, table)
	}
	return filteredTables, nil
}

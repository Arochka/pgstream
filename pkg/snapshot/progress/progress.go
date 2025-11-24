// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"context"
	"fmt"
	"sync"

	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

const defaultAdvanceThreshold = 1 << 20 // 1 MiB

type Progress struct {
	store            snapshotstore.Store
	parser           replication.LSNParser
	threshold        uint64
	mu               sync.RWMutex
	tableLSNs        map[string]map[string]replication.LSN
	pendingTableLSNs map[string]map[string]replication.LSN
}

func New(ctx context.Context, store snapshotstore.Store, parser replication.LSNParser, thresholdBytes uint64) (*Progress, error) {
	if store == nil {
		return nil, fmt.Errorf("snapshot store is required")
	}
	if parser == nil {
		return nil, fmt.Errorf("lsn parser is required")
	}
	if thresholdBytes == 0 {
		thresholdBytes = defaultAdvanceThreshold
	}

	p := &Progress{
		store:            store,
		parser:           parser,
		threshold:        thresholdBytes,
		tableLSNs:        make(map[string]map[string]replication.LSN),
		pendingTableLSNs: make(map[string]map[string]replication.LSN),
	}
	if err := p.load(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Progress) Close() error {
	return p.store.Close()
}

func (p *Progress) load(ctx context.Context) error {
	statuses := []snapshot.Status{snapshot.StatusCompleted, snapshot.StatusInProgress}
	for _, status := range statuses {
		reqs, err := p.store.GetSnapshotRequestsByStatus(ctx, status)
		if err != nil {
			return fmt.Errorf("loading snapshot progress (%s): %w", status, err)
		}
		p.ingestRequests(reqs)
	}
	return nil
}

func (p *Progress) ingestRequests(reqs []*snapshot.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, req := range reqs {
		for _, table := range req.CompletedTables {
			if req.CompletedTableLSNs == nil {
				continue
			}
			lsnStr := req.CompletedTableLSNs[table]
			if lsnStr == "" {
				continue
			}
			lsn, err := p.parser.FromString(lsnStr)
			if err != nil {
				continue
			}
			p.setLSNLocked(req.Schema, table, lsn)
		}
	}
}

func (p *Progress) ShouldSkip(schema, table string, eventLSN replication.LSN) bool {
	if schema == "" || table == "" || eventLSN == 0 {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if tableMap, found := p.tableLSNs[schema]; found {
		if lsn, ok := tableMap[table]; ok && eventLSN <= lsn {
			return true
		}
		if lsn, ok := tableMap["*"]; ok && eventLSN <= lsn {
			return true
		}
	}
	if pending, ok := p.pendingTableLSNs[schema]; ok {
		if lsn, ok := pending[table]; ok && eventLSN <= lsn {
			return true
		}
	}
	return false
}

func (p *Progress) RecordSnapshotCompletion(schema, table, lsnStr string) {
	if schema == "" || table == "" || lsnStr == "" {
		return
	}
	lsn, err := p.parser.FromString(lsnStr)
	if err != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setLSNLocked(schema, table, lsn)
	p.clearPendingLocked(schema, table)
}

func (p *Progress) RecordReplicationLSN(ctx context.Context, schema, table string, lsn replication.LSN) error {
	if schema == "" || table == "" || lsn == 0 {
		return nil
	}
	if !p.shouldPersist(schema, table, lsn) {
		return nil
	}
	lsnStr := p.parser.ToString(lsn)
	if err := p.store.MarkTableCompleted(ctx, schema, table, lsnStr); err != nil {
		return err
	}
	p.mu.Lock()
	p.setLSNLocked(schema, table, lsn)
	p.mu.Unlock()
	return nil
}

func (p *Progress) shouldPersist(schema, table string, lsn replication.LSN) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if tableMap, ok := p.tableLSNs[schema]; ok {
		if prev, ok := tableMap[table]; ok {
			if lsn <= prev {
				return false
			}
			if uint64(lsn-prev) < p.threshold {
				return false
			}
		}
	}
	return true
}

func (p *Progress) setLSNLocked(schema, table string, lsn replication.LSN) {
	if _, found := p.tableLSNs[schema]; !found {
		p.tableLSNs[schema] = make(map[string]replication.LSN)
	}
	p.tableLSNs[schema][table] = lsn
}

// AddPendingDelta marks a table as temporarily skipped until the provided LSN
// is reached, without persisting progress.
func (p *Progress) AddPendingDelta(schema, table string, lsn replication.LSN) {
	if schema == "" || table == "" || lsn == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, found := p.pendingTableLSNs[schema]; !found {
		p.pendingTableLSNs[schema] = make(map[string]replication.LSN)
	}
	if existing, ok := p.pendingTableLSNs[schema][table]; !ok || lsn > existing {
		p.pendingTableLSNs[schema][table] = lsn
	}
}

// ClearPendingDelta removes the temporary skip window associated with a table.
func (p *Progress) ClearPendingDelta(schema, table string) {
	if schema == "" || table == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clearPendingLocked(schema, table)
}

func (p *Progress) clearPendingLocked(schema, table string) {
	if pending, found := p.pendingTableLSNs[schema]; found {
		delete(pending, table)
		if len(pending) == 0 {
			delete(p.pendingTableLSNs, schema)
		}
	}
}

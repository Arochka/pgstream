// SPDX-License-Identifier: Apache-2.0

package deltaqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xataio/pgstream/pkg/snapshot"
	snapshotstore "github.com/xataio/pgstream/pkg/snapshot/store"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

var ErrNoCompletedTables = errors.New("no completed tables recorded in snapshot store; run a full snapshot first")

type Table struct {
	Schema string
	Name   string
}

// ParseTablePatterns converts CLI-style schema.table patterns into structured tables.
func ParseTablePatterns(patterns []string) ([]Table, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("delta snapshots require at least one table")
	}
	res := make([]Table, 0, len(patterns))
	seen := make(map[string]struct{})
	for _, entry := range patterns {
		if entry == "" {
			continue
		}
		schema := "public"
		table := entry
		if strings.Contains(entry, ".") {
			parts := strings.SplitN(entry, ".", 2)
			schema = parts[0]
			table = parts[1]
		}
		if table == "" {
			return nil, fmt.Errorf("delta snapshots require fully qualified table names, got %q", entry)
		}
		key := schema + "." + table
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		res = append(res, Table{Schema: schema, Name: table})
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("no valid tables provided for delta snapshots")
	}
	return res, nil
}

// ExpandTables resolves wildcard table patterns using the snapshot store metadata.
func ExpandTables(ctx context.Context, store snapshotstore.Store, base []Table) ([]Table, error) {
	needsExpansion := false
	for _, tbl := range base {
		if tbl.Name == "*" {
			needsExpansion = true
			break
		}
	}
	if !needsExpansion {
		return base, nil
	}

	tableMap, err := listSnapshotTables(ctx, store)
	if err != nil {
		return nil, err
	}
	if len(tableMap) == 0 {
		return nil, ErrNoCompletedTables
	}

	var expanded []Table
	seen := make(map[string]struct{})
	appendTable := func(schema, table string) {
		if schema == "" || table == "" {
			return
		}
		key := schema + "." + table
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		expanded = append(expanded, Table{Schema: schema, Name: table})
	}

	for _, tbl := range base {
		if tbl.Name != "*" {
			appendTable(tbl.Schema, tbl.Name)
			continue
		}
		switch tbl.Schema {
		case "*", "":
			for schema, tables := range tableMap {
				for table := range tables {
					appendTable(schema, table)
				}
			}
		default:
			if tables, ok := tableMap[tbl.Schema]; ok {
				for table := range tables {
					appendTable(tbl.Schema, table)
				}
			}
		}
	}

	if len(expanded) == 0 {
		return nil, fmt.Errorf("no matching tables found for wildcard delta selection")
	}
	return expanded, nil
}

// LatestCompletedTableLSN returns the highest recorded LSN for the specified table.
func LatestCompletedTableLSN(ctx context.Context, store snapshotstore.Store, parser replication.LSNParser, schema, table string) (string, error) {
	candidates := []string{schema}
	if schema != "*" {
		candidates = append(candidates, "*")
	}

	var best replication.LSN
	found := false
	for _, schemaKey := range candidates {
		reqs, err := store.GetSnapshotRequestsBySchema(ctx, schemaKey)
		if err != nil {
			return "", fmt.Errorf("retrieving snapshot requests for schema %s: %w", schemaKey, err)
		}
		for _, req := range reqs {
			if req == nil || req.CompletedTableLSNs == nil {
				continue
			}
			lsnStr := req.CompletedTableLSNs[table]
			if lsnStr == "" {
				continue
			}
			lsn, err := parser.FromString(lsnStr)
			if err != nil {
				return "", fmt.Errorf("parsing completed LSN for %s.%s: %w", schema, table, err)
			}
			if !found || lsn > best {
				best = lsn
				found = true
			}
		}
	}

	if !found {
		return "", fmt.Errorf("no completed snapshot found for %s.%s", schema, table)
	}
	return parser.ToString(best), nil
}

func listSnapshotTables(ctx context.Context, store snapshotstore.Store) (map[string]map[string]struct{}, error) {
	result := make(map[string]map[string]struct{})
	statuses := []snapshot.Status{snapshot.StatusCompleted, snapshot.StatusInProgress}
	for _, status := range statuses {
		reqs, err := store.GetSnapshotRequestsByStatus(ctx, status)
		if err != nil {
			return nil, fmt.Errorf("loading %s snapshot requests: %w", status, err)
		}
		for _, req := range reqs {
			if req == nil || req.Schema == "" {
				continue
			}
			if isTemporarySchema(req.Schema) {
				continue
			}
			if _, ok := result[req.Schema]; !ok {
				result[req.Schema] = make(map[string]struct{})
			}
			for table := range req.CompletedTableLSNs {
				if table != "" {
					result[req.Schema][table] = struct{}{}
				}
			}
			for _, table := range req.CompletedTables {
				if table != "" {
					result[req.Schema][table] = struct{}{}
				}
			}
		}
	}
	return result, nil
}

func isTemporarySchema(schema string) bool {
	return strings.HasPrefix(schema, "pg_temp_")
}

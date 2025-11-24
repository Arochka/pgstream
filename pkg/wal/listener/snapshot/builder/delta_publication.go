// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
)

var publicationLocks sync.Map

func ensureDeltaPublication(ctx context.Context, cfg *SnapshotListenerConfig, logger loglib.Logger) error {
	if cfg == nil || cfg.Delta == nil || cfg.Delta.PublicationName == "" || cfg.Data == nil || cfg.Data.URL == "" {
		return nil
	}

	lock := acquirePublicationLock(cfg.Delta.PublicationName)
	lock.Lock()
	defer lock.Unlock()

	conn, err := pglib.NewConn(ctx, cfg.Data.URL)
	if err != nil {
		return fmt.Errorf("connecting to postgres for publication management: %w", err)
	}
	defer conn.Close(ctx)

	statements, err := planPublicationStatements(ctx, conn, cfg.Adapter.Tables, cfg.Adapter.ExcludedTables, cfg.Delta.PublicationName)
	if err != nil {
		return err
	}

	autoCfg := cfg.Delta.AutoPublication
	if autoCfg != nil && len(autoCfg.CustomSQL) > 0 {
		statements = append([]string{}, autoCfg.CustomSQL...)
	}

	if autoCfg != nil && autoCfg.Enabled {
		for _, stmt := range statements {
			if strings.TrimSpace(stmt) == "" {
				continue
			}
			if _, execErr := conn.Exec(ctx, stmt); execErr != nil {
				return fmt.Errorf("executing publication SQL %q: %w", stmt, execErr)
			}
		}
		logger.Info("delta publication managed automatically", loglib.Fields{
			"publication": cfg.Delta.PublicationName,
			"statements":  statements,
		})
		return nil
	}

	logger.Info("delta publication auto-management disabled; execute SQL manually", loglib.Fields{
		"publication": cfg.Delta.PublicationName,
		"statements":  statements,
	})
	return nil
}

func acquirePublicationLock(name string) *sync.Mutex {
	val, _ := publicationLocks.LoadOrStore(name, &sync.Mutex{})
	return val.(*sync.Mutex)
}

func planPublicationStatements(ctx context.Context, conn pglib.Querier, includePatterns, excludePatterns []string, publication string) ([]string, error) {
	tables, err := listAllTables(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("listing tables for publication: %w", err)
	}
	return buildPublicationStatements(tables, includePatterns, excludePatterns, publication)
}

func buildPublicationStatements(tables []tableRef, includePatterns, excludePatterns []string, publication string) ([]string, error) {
	includes := parseIncludePatterns(includePatterns)
	excludes := parseExcludePatterns(excludePatterns)
	selected := filterTables(tables, includes, excludes)
	if len(selected) == 0 {
		return nil, fmt.Errorf("no tables match snapshot delta include patterns; update source.postgres.snapshot.tables")
	}
	names := make([]string, 0, len(selected))
	for _, tbl := range selected {
		names = append(names, pglib.QuoteQualifiedIdentifier(tbl.schema, tbl.name))
	}
	sort.Strings(names)
	create := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pglib.QuoteIdentifier(publication), strings.Join(names, ", "))
	statements := []string{
		fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pglib.QuoteIdentifier(publication)),
		create,
	}
	return statements, nil
}

type tableRef struct {
	schema      string
	name        string
	schemaLower string
	nameLower   string
}

func listAllTables(ctx context.Context, conn pglib.Querier) ([]tableRef, error) {
	rows, err := conn.Query(ctx, `SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
  AND table_schema NOT LIKE 'pg_toast%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []tableRef
	for rows.Next() {
		var schema, name string
		if scanErr := rows.Scan(&schema, &name); scanErr != nil {
			return nil, scanErr
		}
		tables = append(tables, tableRef{
			schema:      schema,
			name:        name,
			schemaLower: strings.ToLower(schema),
			nameLower:   strings.ToLower(name),
		})
	}
	return tables, rows.Err()
}

type tablePattern struct {
	schema string
	table  string
}

func parseIncludePatterns(entries []string) []tablePattern {
	patterns := parsePatterns(entries)
	if len(patterns) == 0 {
		return []tablePattern{{schema: "*", table: "*"}}
	}
	return patterns
}

func parseExcludePatterns(entries []string) []tablePattern {
	return parsePatterns(entries)
}

func parsePatterns(entries []string) []tablePattern {
	patterns := make([]tablePattern, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		schema := "public"
		var table string
		if entry == "*" || entry == "*.*" {
			schema = "*"
			table = "*"
		} else if strings.Contains(entry, ".") {
			parts := strings.SplitN(entry, ".", 2)
			if parts[0] != "" {
				schema = strings.ToLower(parts[0])
			} else {
				schema = "*"
			}
			if parts[1] != "" {
				table = strings.ToLower(parts[1])
			} else {
				table = "*"
			}
		} else {
			table = strings.ToLower(entry)
		}
		patterns = append(patterns, tablePattern{
			schema: strings.ToLower(schema),
			table:  strings.ToLower(table),
		})
	}
	return patterns
}

func filterTables(tables []tableRef, includes, excludes []tablePattern) []tableRef {
	var result []tableRef
	for _, tbl := range tables {
		if matchesAny(tbl, includes) && !matchesAny(tbl, excludes) {
			result = append(result, tbl)
		}
	}
	return result
}

func matchesAny(tbl tableRef, patterns []tablePattern) bool {
	for _, p := range patterns {
		if matchesPattern(tbl, p) {
			return true
		}
	}
	return false
}

func matchesPattern(tbl tableRef, pattern tablePattern) bool {
	schemaMatch := pattern.schema == "*" || tbl.schemaLower == pattern.schema
	tableMatch := pattern.table == "*" || tbl.nameLower == pattern.table
	return schemaMatch && tableMatch
}

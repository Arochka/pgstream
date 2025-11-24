// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildPublicationStatements_AllTablesWithExcludes(t *testing.T) {
	tables := []tableRef{
		newTableRef("public", "users"),
		newTableRef("pgstream", "schema_log"),
		newTableRef("sales", "orders"),
	}

	stmts, err := buildPublicationStatements(tables, []string{"*.*"}, []string{"pgstream.*"}, "pgstream_delta")
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	require.Equal(t, `DROP PUBLICATION IF EXISTS "pgstream_delta"`, stmts[0])
	require.Equal(t, `CREATE PUBLICATION "pgstream_delta" FOR TABLE "public"."users", "sales"."orders"`, stmts[1])
}

func TestBuildPublicationStatements_SpecificTables(t *testing.T) {
	tables := []tableRef{
		newTableRef("public", "users"),
		newTableRef("public", "accounts"),
	}
	includes := parseIncludePatterns([]string{"public.users"})
	excludes := parseExcludePatterns(nil)
	require.Len(t, filterTables(tables, includes, excludes), 1)

	stmts, err := buildPublicationStatements(tables, []string{"public.users"}, nil, "pgstream_delta")
	require.NoError(t, err)
	require.Equal(t, `CREATE PUBLICATION "pgstream_delta" FOR TABLE "public"."users"`, stmts[1])
}

func TestBuildPublicationStatements_NoMatches(t *testing.T) {
	tables := []tableRef{
		newTableRef("public", "users"),
	}

	_, err := buildPublicationStatements(tables, []string{"sales.*"}, nil, "pgstream_delta")
	require.Error(t, err)
}

func newTableRef(schema, table string) tableRef {
	return tableRef{
		schema:      schema,
		name:        table,
		schemaLower: strings.ToLower(schema),
		nameLower:   strings.ToLower(table),
	}
}

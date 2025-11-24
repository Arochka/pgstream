// SPDX-License-Identifier: Apache-2.0

package builder

import (
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/snapshot"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
)

type SnapshotListenerConfig struct {
	Data                    *pgsnapshotgenerator.Config
	Adapter                 adapter.SnapshotConfig
	Recorder                *SnapshotRecorderConfig
	Schema                  *SchemaSnapshotConfig
	Delta                   *SnapshotDeltaConfig
	DisableProgressTracking bool
}

type SchemaSnapshotConfig struct {
	SchemaLogStore *schemalogpg.Config
	DumpRestore    *pgdumprestore.Config
}

type SnapshotRecorderConfig struct {
	RepeatableSnapshots bool
	SnapshotStoreURL    string
}

type SnapshotDeltaConfig = snapshot.DeltaConfig

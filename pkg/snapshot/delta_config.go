// SPDX-License-Identifier: Apache-2.0

package snapshot

import "time"

// DeltaConfig controls how incremental snapshot replays are scheduled.
type DeltaConfig struct {
	// MaxTablesPerRun limits how many tables can be replayed in a single delta
	// pass. Zero or negative values disable the limit.
	MaxTablesPerRun int
	// LagThresholdBytes defines the minimum WAL distance (in bytes) required
	// before planning a delta for a table. Zero disables the check.
	LagThresholdBytes uint64
	// PublicationName identifies the Postgres publication that exposes row-level
	// changes for delta replays when using the pgoutput plugin.
	PublicationName string
	// SlotName allows reusing an existing logical replication slot (for example
	// the streaming slot) instead of creating a temporary one per delta run.
	// When empty, the delta reader will create and drop a dedicated slot.
	SlotName string
	// ReplayTimeout limits how long a single delta replay run can take. When
	// zero, delta replays run until completion unless the parent context is
	// canceled.
	ReplayTimeout time.Duration
	// AutoPublication defines whether pgstream should manage the pgoutput
	// publication automatically when delta mode is enabled.
	AutoPublication *AutoPublicationConfig
}

// AutoPublicationConfig controls how the publication used by the delta reader
// is created.
type AutoPublicationConfig struct {
	// Enabled toggles whether pgstream should drop/recreate the publication
	// during startup. When false, pgstream will only print the SQL statements
	// that should be executed manually.
	Enabled bool
	// CustomSQL overrides the SQL statements pgstream would normally generate
	// for managing the publication. Statements are executed in order when
	// Enabled is true.
	CustomSQL []string
}

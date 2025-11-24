// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"fmt"
	"strings"
)

type Status struct {
	Init                *InitStatus
	Config              *ConfigStatus
	TransformationRules *TransformationRulesStatus
	Source              *SourceStatus
	Delta               *DeltaStatus
}

type ConfigStatus struct {
	Valid  bool
	Errors []string
}

type TransformationRulesStatus struct {
	Valid  bool
	Errors []string
}

type SourceStatus struct {
	Reachable bool
	Errors    []string
}

type DeltaStatus struct {
	Pending    []DeltaRequestStatus
	InProgress []DeltaRequestStatus
}

type DeltaRequestStatus struct {
	Schema   string   `json:"schema"`
	Tables   []string `json:"tables"`
	StartLSN string   `json:"start_lsn,omitempty"`
	EndLSN   string   `json:"end_lsn,omitempty"`
	LagBytes uint64   `json:"lag_bytes,omitempty"`
}

type InitStatus struct {
	PgstreamSchema  *SchemaStatus
	Migration       *MigrationStatus
	ReplicationSlot *ReplicationSlotStatus
}

type MigrationStatus struct {
	Version uint
	Dirty   bool
	Errors  []string
}

type ReplicationSlotStatus struct {
	Name     string
	Plugin   string
	Database string
	Errors   []string
}

type SchemaStatus struct {
	SchemaExists         bool
	SchemaLogTableExists bool
	Errors               []string
}

type StatusErrors map[string][]string

func (se StatusErrors) Keys() []string {
	keys := make([]string, 0, len(se))
	for k := range se {
		keys = append(keys, k)
	}
	return keys
}

func (s *Status) GetErrors() StatusErrors {
	if s == nil {
		return nil
	}

	errors := map[string][]string{}
	if initErrs := s.Init.GetErrors(); len(initErrs) > 0 {
		errors["init"] = initErrs
	}

	if s.Source != nil && len(s.Source.Errors) > 0 {
		errors["source"] = s.Source.Errors
	}

	if s.Config != nil && len(s.Config.Errors) > 0 {
		errors["config"] = s.Config.Errors
	}

	if s.TransformationRules != nil && len(s.TransformationRules.Errors) > 0 {
		errors["transformation_rules"] = s.TransformationRules.Errors
	}

	return errors
}

func (s *Status) PrettyPrint() string {
	if s == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString(s.Init.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.Config.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.TransformationRules.PrettyPrint())
	prettyPrint.WriteByte('\n')
	prettyPrint.WriteString(s.Source.PrettyPrint())
	if s.Delta != nil {
		prettyPrint.WriteByte('\n')
		prettyPrint.WriteString(s.Delta.PrettyPrint())
	}

	return prettyPrint.String()
}

func (d *DeltaStatus) PrettyPrint() string {
	if d == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("Delta snapshots:\n")
	if len(d.Pending) == 0 && len(d.InProgress) == 0 {
		b.WriteString(" - Queue empty")
		return b.String()
	}

	if len(d.Pending) == 0 {
		b.WriteString(" - Pending: none\n")
	} else {
		b.WriteString(" - Pending:\n")
		for _, req := range d.Pending {
			b.WriteString(formatDeltaRequest(req))
		}
	}

	if len(d.InProgress) == 0 {
		b.WriteString(" - In progress: none")
	} else {
		b.WriteString(" - In progress:\n")
		for _, req := range d.InProgress {
			b.WriteString(formatDeltaRequest(req))
		}
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func formatDeltaRequest(req DeltaRequestStatus) string {
	var b strings.Builder
	b.WriteString("    * ")
	if req.Schema != "" {
		b.WriteString(req.Schema)
	} else {
		b.WriteString("<unknown-schema>")
	}
	if len(req.Tables) > 0 {
		b.WriteString(".")
		b.WriteString(strings.Join(req.Tables, ","))
	}
	if req.StartLSN != "" || req.EndLSN != "" {
		b.WriteString(" [")
		b.WriteString(req.StartLSN)
		if req.EndLSN != "" {
			b.WriteString(" -> ")
			b.WriteString(req.EndLSN)
		}
		b.WriteString("]")
	}
	if req.LagBytes > 0 {
		b.WriteString(fmt.Sprintf(" lag=%dB", req.LagBytes))
	}
	b.WriteByte('\n')
	return b.String()
}

// GetErrors aggregates all errors from the initialisation status.
func (is *InitStatus) GetErrors() []string {
	if is == nil {
		return nil
	}

	errors := []string{}
	if is.PgstreamSchema != nil && len(is.PgstreamSchema.Errors) > 0 {
		errors = append(errors, is.PgstreamSchema.Errors...)
	}

	if is.Migration != nil && len(is.Migration.Errors) > 0 {
		errors = append(errors, is.Migration.Errors...)
	}

	if is.ReplicationSlot != nil && len(is.ReplicationSlot.Errors) > 0 {
		errors = append(errors, is.ReplicationSlot.Errors...)
	}

	return errors
}

func (is *InitStatus) PrettyPrint() string {
	if is == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Initialisation status:\n")
	if is.PgstreamSchema != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema exists: %t\n", is.PgstreamSchema.SchemaExists))
		prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema_log table exists: %t\n", is.PgstreamSchema.SchemaLogTableExists))
		if len(is.PgstreamSchema.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Pgstream schema errors: %s\n", is.PgstreamSchema.Errors))
		}
	}

	if is.Migration != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Migration current version: %d\n", is.Migration.Version))
		prettyPrint.WriteString(fmt.Sprintf(" - Migration status: %s\n", migrationStatus(is.Migration.Dirty)))
		if len(is.Migration.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Migration errors: %s\n", is.Migration.Errors))
		}
	}

	if is.ReplicationSlot != nil {
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot name: %s\n", is.ReplicationSlot.Name))
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot plugin: %s\n", is.ReplicationSlot.Plugin))
		prettyPrint.WriteString(fmt.Sprintf(" - Replication slot database: %s\n", is.ReplicationSlot.Database))
		if len(is.ReplicationSlot.Errors) > 0 {
			prettyPrint.WriteString(fmt.Sprintf(" - Replication slot errors: %s\n", is.ReplicationSlot.Errors))
		}
	}

	// trim the last newline character
	return strings.TrimSuffix(prettyPrint.String(), "\n")
}

func (ss *SourceStatus) PrettyPrint() string {
	if ss == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Source status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Reachable: %t\n", ss.Reachable))
	if len(ss.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", ss.Errors))
	}

	// trim the last newline character
	return strings.TrimSuffix(prettyPrint.String(), "\n")
}

func (cs *ConfigStatus) PrettyPrint() string {
	if cs == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Config status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Valid: %t\n", cs.Valid))
	if len(cs.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", cs.Errors))
	}

	// trim the last newline character
	return strings.TrimSuffix(prettyPrint.String(), "\n")
}

func (trs *TransformationRulesStatus) PrettyPrint() string {
	if trs == nil {
		return ""
	}

	var prettyPrint strings.Builder
	prettyPrint.WriteString("Transformation rules status:\n")
	prettyPrint.WriteString(fmt.Sprintf(" - Valid: %t\n", trs.Valid))
	if len(trs.Errors) > 0 {
		prettyPrint.WriteString(fmt.Sprintf(" - Errors: %s\n", trs.Errors))
	}

	// trim the last newline character
	return strings.TrimSuffix(prettyPrint.String(), "\n")
}

func migrationStatus(dirty bool) string {
	if !dirty {
		return "success"
	}
	return "failed"
}

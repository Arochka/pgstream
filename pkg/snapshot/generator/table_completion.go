// SPDX-License-Identifier: Apache-2.0

package generator

import "context"

// TableCompletionHook is invoked whenever a snapshot generator finishes
// snapshotting a table successfully.
type TableCompletionHook func(ctx context.Context, schema, table, lsn string)

// TableCompletionNotifier can be implemented by snapshot generators that can
// notify when a table has been snapshotted.
type TableCompletionNotifier interface {
	SetTableCompletionHook(TableCompletionHook)
}

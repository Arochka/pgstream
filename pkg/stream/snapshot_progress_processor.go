// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"

	loglib "github.com/xataio/pgstream/pkg/log"
	snapshotprogress "github.com/xataio/pgstream/pkg/snapshot/progress"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

type snapshotAwareProcessor struct {
	inner    processor.Processor
	progress *snapshotprogress.Progress
	parser   replication.LSNParser
	logger   loglib.Logger
}

func newSnapshotProgressProcessor(inner processor.Processor, progress *snapshotprogress.Progress, parser replication.LSNParser, logger loglib.Logger) processor.Processor {
	if progress == nil || parser == nil {
		return inner
	}
	return &snapshotAwareProcessor{
		inner:    inner,
		progress: progress,
		parser:   parser,
		logger:   logger,
	}
}

func (s *snapshotAwareProcessor) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	if event == nil {
		return s.inner.ProcessWALEvent(ctx, event)
	}

	data := event.Data
	if data == nil || data.Schema == "" || data.Table == "" || processor.IsSchemaLogEvent(data) {
		return s.inner.ProcessWALEvent(ctx, event)
	}

	lsn, err := s.parser.FromString(string(event.CommitPosition))
	if err != nil || lsn == 0 {
		return s.inner.ProcessWALEvent(ctx, event)
	}

	if s.progress.ShouldSkip(data.Schema, data.Table, lsn) {
		return nil
	}

	if err := s.inner.ProcessWALEvent(ctx, event); err != nil {
		return err
	}

	if err := s.progress.RecordReplicationLSN(ctx, data.Schema, data.Table, lsn); err != nil {
		s.logger.Warn(err, "recording replication snapshot progress", nil)
	}

	return nil
}

func (s *snapshotAwareProcessor) Close() error {
	return s.inner.Close()
}

func (s *snapshotAwareProcessor) Name() string {
	return s.inner.Name()
}

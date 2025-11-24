// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

type Config struct {
	Listener  ListenerConfig
	Processor ProcessorConfig
}

type ListenerConfig struct {
	Postgres *PostgresListenerConfig
	Kafka    *KafkaListenerConfig
}

type PostgresListenerConfig struct {
	URL              string
	Replication      pgreplication.Config
	RetryPolicy      backoff.Config
	Snapshot         *snapshotbuilder.SnapshotListenerConfig
	SnapshotStoreURL string
}

type KafkaListenerConfig struct {
	Reader       kafka.ReaderConfig
	Checkpointer kafkacheckpoint.Config
}

type ProcessorConfig struct {
	Kafka       *KafkaProcessorConfig
	Search      *SearchProcessorConfig
	Webhook     *WebhookProcessorConfig
	Postgres    *PostgresProcessorConfig
	Injector    *injector.Config
	Transformer *transformer.Config
	Filter      *filter.Config
}

type KafkaProcessorConfig struct {
	Writer *kafkaprocessor.Config
}

type SearchProcessorConfig struct {
	Indexer search.IndexerConfig
	Store   store.Config
	Retrier search.StoreRetryConfig
}

type WebhookProcessorConfig struct {
	Notifier           notifier.Config
	SubscriptionServer server.Config
	SubscriptionStore  WebhookSubscriptionStoreConfig
}

type PostgresProcessorConfig struct {
	BatchWriter postgres.Config
}

type WebhookSubscriptionStoreConfig struct {
	URL                  string
	CacheEnabled         bool
	CacheRefreshInterval time.Duration
}

func (c *Config) IsValid() error {
	if err := c.Listener.IsValid(); err != nil {
		return err
	}

	return c.Processor.IsValid()
}

func (c *ListenerConfig) IsValid() error {
	listenerCount := 0
	if c.Kafka != nil {
		listenerCount++
	}
	if c.Postgres != nil {
		listenerCount++
		if err := c.Postgres.validate(); err != nil {
			return err
		}
	}

	switch listenerCount {
	case 0:
		return errors.New("need at least one listener configured")
	case 1:
		// Only one listener is configured, do nothing
		return nil
	default:
		// More than one listener is configured, return an error
		return fmt.Errorf("only one listener can be configured at a time, found %d", listenerCount)
	}
}

func (c *PostgresListenerConfig) validate() error {
	if c == nil || c.Snapshot == nil || c.Snapshot.Delta == nil {
		return nil
	}
	if c.SnapshotStoreURL == "" {
		return fmt.Errorf("delta snapshots require a snapshot store (set source.postgres.snapshot.recorder.postgres_url or PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL)")
	}
	if len(c.Snapshot.Adapter.Tables) == 0 {
		return fmt.Errorf("delta snapshots require at least one table pattern in source.postgres.snapshot.tables")
	}
	if c.Snapshot.Delta.PublicationName == "" {
		return fmt.Errorf("delta snapshots require a pgoutput publication name (source.postgres.snapshot.delta.publication_name)")
	}
	return nil
}

func (c *ProcessorConfig) IsValid() error {
	processorCount := 0
	if c.Kafka != nil {
		processorCount++
	}
	if c.Postgres != nil {
		processorCount++
	}
	if c.Search != nil {
		processorCount++
	}
	if c.Webhook != nil {
		processorCount++
	}

	switch processorCount {
	case 0:
		return errors.New("need at least one processor configured")
	case 1:
		// Only one processor is configured, do nothing
		return nil
	default:
		// More than one processor is configured, return an error
		return fmt.Errorf("only one processor can be configured at a time, found %d", processorCount)
	}
}

func (c *Config) SourcePostgresURL() string {
	if c.Listener.Postgres != nil {
		return c.Listener.Postgres.URL
	}
	return ""
}

func (c *Config) SnapshotStoreURL() string {
	if c.Listener.Postgres != nil {
		return c.Listener.Postgres.SnapshotStoreURL
	}
	return ""
}

func (c *Config) PostgresReplicationSlot() string {
	if c.Listener.Postgres != nil {
		return c.Listener.Postgres.Replication.ReplicationSlotName
	}
	return ""
}

func (c *Config) RequiredTables() []string {
	requiredTables := []string{}
	if c.Listener.Postgres != nil {
		if c.Listener.Postgres.Snapshot != nil {
			requiredTables = append(requiredTables, c.Listener.Postgres.Snapshot.Adapter.Tables...)
		}
	}
	// TODO: add included tables for the replication case as well
	return requiredTables
}

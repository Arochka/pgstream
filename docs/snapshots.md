# 📷 Snapshots

![snapshots diagram](img/pgstream_snapshot_diagram.svg)

`pgstream` supports the generation of PostgreSQL schema and data snapshots. It can be done as an initial step before starting the replication, or as a standalone mode, where a snapshot of the database is performed without any replication.

The snapshot behaviour is the same in both cases, with the only difference that if we're listening on the replication slot, we will store the current LSN before performing the snapshot, so that we can replay any operations that happened while the snapshot was ongoing.

The snapshot implementation is different for schema and data.

- Schema: depending on the configuration, it can use either the pgstream `schema_log` table to get the schema view and process it as events downstream, or rely on the `pg_dump`/`pg_restore` PostgreSQL utilities if the target is a PostgreSQL database.

- Data: it relies on transaction snapshot ids to obtain a stable view of the database tables, and paralellises the read of all the rows by dividing them into ranges using the `ctid`.

![snapshots sequence](img/pgstream_snapshot_sequence.svg)

For more details into the snapshot implementation and performance benchmarking, check out this [blogpost](https://xata.io/blog/behind-the-scenes-speeding-up-pgstream-snapshots-for-postgresql). For details on how to use and configure the snapshot mode, check the [snapshot tutorial](tutorials/postgres_snapshot.md).

## Incremental ("delta") snapshots

Once a table has been snapshotted and replicated at least once, pgstream can replay only the missing WAL window instead of re-copying the entire table. This is the recommended way to recover from long downtimes: we read the last completed table LSN from the snapshot store, create (or reuse) a pgoutput logical slot, decode the row-level mutations between `start_lsn` and `end_lsn`, and feed them back through the same processors that handle live replication.

```yaml
source:
  postgres:
    snapshot:
      delta:
        max_tables_per_run: 10                    # optional throttling
        lag_threshold_bytes: 16777216             # only enqueue when lag ≥ 16MB
        publication_name: pgstream_delta          # pgoutput publication to read from
        slot_name: pgstream_revers_db_slot        # optional: reuse an existing slot instead of creating temporary ones
        replay_timeout: "0s"                     # optional safety timeout per replay run (0s disables it)
        auto_publication:
          enabled: true                           # drop/recreate the publication during startup
          custom_sql: []                          # optional override of the SQL statements to execute
```

**How it works**

1. When `pgstream snapshot --delta --tables schema.table` is executed (or when the scheduler enqueues a delta request), a row with `mode=delta`, `start_lsn=<last completed>` and `end_lsn=<current wal>` is stored in `pgstream.snapshot_requests`.
2. The snapshot generator sees pending delta requests, creates or reuses a logical slot (based on `slot_name`), and streams pgoutput messages between the requested LSNs. Every INSERT/UPDATE/DELETE is decoded into a `wal.Event` and pushed through the configured processors, so downstream targets receive the exact same mutations they would have seen via live replication.
3. When the replay completes, `completed_table_lsns` is updated for the affected tables, which allows the replication processor (`ShouldSkip`) to ignore duplicate WAL entries as it catches up.

**Operational notes**

- You must create the `publication_name` in Postgres and include every table you plan to replay (e.g. `CREATE PUBLICATION pgstream_delta FOR TABLE public.*`). It must match the schema used by the replication slot.
- If you provide `slot_name`, pgstream will reuse that logical replication slot for every delta run. This is useful when you need the slot to persist across restarts. If you omit it, pgstream will create short-lived slots named `pgstream_delta_<schema>_<table>` and drop them after the replay finishes.
- Ensure `wal_level=logical` and retain enough WAL (`wal_keep_size` or archiving) to cover the maximum expected downtime; the delta reader cannot recover changes that have already been recycled.
- You still need a snapshot recorder (`source.postgres.snapshot.recorder.postgres_url`) so pgstream can persist the table → LSN mapping and queue delta requests automatically when lag exceeds `lag_threshold_bytes`.
- Delta requests are only enqueued during the initial planning phase or by explicitly running `pgstream snapshot --delta`; there is no background scheduler once replication starts.
- `replay_timeout` lets you cap the duration of a single delta replay run. Leave it at `0s` to allow replays to run until completion.
- Setting `auto_publication.enabled` to true tells pgstream to drop/recreate the pgoutput publication on startup so that every table matching `snapshot.tables` (minus `snapshot.excluded_tables`) is included. When the flag is left to false, pgstream simply prints the SQL statements you should run manually. If you provide `auto_publication.custom_sql`, those statements are executed instead (with no SQL generated automatically).
- Because PostgreSQL publications can’t express “all schemas except…”, pgstream enumerates the tables that currently match your patterns. Re-run pgstream (or the auto-publication logic) after major schema changes to keep the publication aligned.
- For a full working example with delta scheduling enabled see `docs/examples/pg2pg_delta.yaml`.

### Monitoring incremental snapshots

- `pgstream status` now reports the backlog section `Delta snapshots`, so operators can confirm whether requests are pending or in progress before starting a catch-up run.
- `pgstream status --json` surfaces the same data in machine-readable form. Example:

```json
{
  "Delta": {
    "pending": [
      {
        "schema": "public",
        "tables": ["customers"],
        "start_lsn": "0/1883980",
        "end_lsn": "0/1883B88",
        "lag_bytes": 6656
      }
    ],
    "in_progress": []
  }
}
```

If the delta block is empty or absent, there are no tables queued for replay. Otherwise you can validate that `start_lsn` and `end_lsn` match the expected recovery window before running `pgstream snapshot --delta` or waiting for the scheduler to kick in.

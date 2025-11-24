
DO $$
BEGIN
    IF to_regclass('pgstream.snapshot_requests') IS NULL THEN
        RETURN;
    END IF;

    EXECUTE 'DROP INDEX IF EXISTS pgstream.schema_table_status_unique_index';

    EXECUTE '
        CREATE UNIQUE INDEX schema_table_status_unique_index
        ON pgstream.snapshot_requests(schema_name, table_names)
        WHERE status != ''completed'' AND mode != ''delta''
    ';
END
$$;

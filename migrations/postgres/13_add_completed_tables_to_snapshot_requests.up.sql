DO $$
BEGIN
    IF to_regclass('pgstream.snapshot_requests') IS NULL THEN
        RETURN;
    END IF;

    EXECUTE '
        ALTER TABLE pgstream.snapshot_requests
            ADD COLUMN IF NOT EXISTS completed_tables TEXT[] NOT NULL DEFAULT ''{}'';
    ';

    EXECUTE '
        ALTER TABLE pgstream.snapshot_requests
            ADD COLUMN IF NOT EXISTS completed_table_lsns JSONB NOT NULL DEFAULT ''{}''::jsonb;
    ';
END
$$;

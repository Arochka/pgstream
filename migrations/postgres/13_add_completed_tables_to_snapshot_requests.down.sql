DO $$
BEGIN
    IF to_regclass('pgstream.snapshot_requests') IS NULL THEN
        RETURN;
    END IF;

    EXECUTE '
        ALTER TABLE pgstream.snapshot_requests
            DROP COLUMN IF EXISTS completed_table_lsns;
    ';

    EXECUTE '
        ALTER TABLE pgstream.snapshot_requests
            DROP COLUMN IF EXISTS completed_tables;
    ';
END
$$;

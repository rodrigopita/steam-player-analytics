CREATE TABLE IF NOT EXISTS raw.snapshot_runs (
    run_id               text        PRIMARY KEY,
    logical_hour         timestamptz NOT NULL,
    tracked_count        integer,
    observed_count       integer,
    -- null means not recorded, no bundle or one older than the field; '{}' means all reported
    unobservable_app_ids bigint[],
    loaded_count         integer     NOT NULL,
    latency_p50_ms       integer,
    latency_max_ms       integer,
    s3_key               text,
    recorded_at          timestamptz NOT NULL DEFAULT now()
)

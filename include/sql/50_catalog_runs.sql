CREATE TABLE IF NOT EXISTS raw.catalog_runs (
    run_id                  text        PRIMARY KEY,
    logical_date            timestamptz NOT NULL,
    app_list_count          integer,
    chart_count             integer,
    skipped_chart_app_ids   bigint[],
    newly_tracked_count     integer,
    reactivated_count       integer,
    deactivated_count       integer,
    no_metadata_app_ids     bigint[],
    metadata_upserted_count integer,
    app_list_key            text,
    most_played_key         text,
    recorded_at             timestamptz NOT NULL DEFAULT now()
)

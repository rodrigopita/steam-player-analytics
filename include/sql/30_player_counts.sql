CREATE TABLE IF NOT EXISTS raw.player_counts (
    app_id        bigint      NOT NULL,
    player_count  integer     NOT NULL,
    logical_hour  timestamptz NOT NULL,
    observed_at   timestamptz,
    s3_key        text        NOT NULL,
    loaded_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (app_id, logical_hour)
)

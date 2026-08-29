CREATE TABLE IF NOT EXISTS raw.app_metadata (
    app_id          bigint      PRIMARY KEY,
    data            jsonb       NOT NULL,
    first_loaded_at timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
)

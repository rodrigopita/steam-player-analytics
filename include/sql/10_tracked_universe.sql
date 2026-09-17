CREATE TABLE IF NOT EXISTS raw.tracked_universe (
    app_id       bigint      PRIMARY KEY,
    name         text        NOT NULL,
    source       text        NOT NULL,
    tracked_from timestamptz NOT NULL DEFAULT now(),
    is_active    boolean     NOT NULL DEFAULT true
)
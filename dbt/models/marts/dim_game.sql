WITH
universe AS (
    SELECT *
    FROM {{ ref('stg_tracked_universe') }}
),

metadata AS (
    SELECT *
    FROM {{ ref('stg_app_metadata') }}
),

runs AS (
    SELECT *
    FROM {{ ref('stg_snapshot_runs') }}
),

recent AS (
    SELECT *
    FROM runs
    WHERE unobservable_app_ids IS NOT NULL
    ORDER BY logical_hour DESC
    LIMIT {{ var("known_unobservable_hours") }}
),

unobservable AS (
    SELECT
        app_id
    FROM recent, unnest(unobservable_app_ids) AS u(app_id)
    GROUP BY app_id
    HAVING COUNT(*) = {{ var("known_unobservable_hours") }}
),

joined AS (
    SELECT
        u.app_id,
        COALESCE(m.app_name, u.app_name) AS app_name,
        m.app_type,
        m.app_description,
        m.release_date,
        m.genre_names,
        m.required_age,
        m.is_free,
        m.is_windows,
        m.is_mac,
        m.is_linux,
        m.metacritic_score,
        m.achievement_count,
        m.recommendation_count,
        m.price_initial_usd,
        m.price_final_usd,
        u.tracked_source,
        u.tracked_from,
        u.is_active,
        un.app_id IS NOT NULL AS is_unobservable
    FROM universe u
    LEFT JOIN metadata m
        ON u.app_id = m.app_id
    LEFT JOIN unobservable un
        ON u.app_id = un.app_id
)

SELECT *
FROM joined

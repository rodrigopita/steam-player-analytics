WITH
universe AS (
    SELECT *
    FROM {{ ref('stg_tracked_universe') }}
),

metadata AS (
    SELECT *
    FROM {{ ref('stg_app_metadata') }}
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
        u.is_active
    FROM universe u
    LEFT JOIN metadata m
        ON u.app_id = m.app_id
)

SELECT *
FROM joined

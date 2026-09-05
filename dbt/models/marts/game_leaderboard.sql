WITH
games AS (
    SELECT *
    FROM {{ ref('dim_game') }}
    WHERE is_active
),

observations AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY app_id ORDER BY logical_hour DESC) AS rn
    FROM {{ ref('fct_player_counts') }}
),

latest AS (
    SELECT *
    FROM observations
    WHERE rn = 1
),

trends AS (
    SELECT *
    FROM {{ ref('fct_player_trends_daily') }}
),

ranked AS (
    SELECT
        g.app_id,
        g.app_name,
        (DENSE_RANK() OVER (ORDER BY l.player_count DESC))::int AS current_rank,
        l.player_count AS current_player_count,
        l.logical_hour AS current_hour,
        t.avg_player_count AS avg_player_count_current_day,
        t.peak_player_count AS peak_player_count_current_day,
        t.observed_hours AS observed_hours_current_day,
        (DENSE_RANK() OVER (ORDER BY t.avg_player_count_7d DESC))::int AS rank_7d,
        t.avg_player_count_7d,
        t.observed_days_7d,
        t.peak_player_count_record,
        t.last_record_day,
        t.days_since_peak,
        g.release_date,
        g.genre_names,
        g.is_free,
        g.price_final_usd,
        g.tracked_source
    FROM games g
    JOIN latest l
        ON g.app_id = l.app_id
    JOIN trends t
        ON g.app_id = t.app_id
        AND l.date_day = t.date_day
)

SELECT *
FROM ranked

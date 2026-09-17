WITH
snapshots AS (
    SELECT *
    FROM {{ ref('fct_player_counts') }}
),

aggregated AS (
    SELECT
        app_id,
        date_day,
        ROUND(AVG(player_count), 1) AS avg_player_count,
        MAX(player_count) AS peak_player_count,
        COUNT(*)::int AS observed_hours
    FROM snapshots
    GROUP BY 1, 2
),

first_observed AS (
    SELECT
        app_id,
        MIN(date_day) AS first_observed_day
    FROM snapshots
    GROUP BY 1
),

spine AS (
    SELECT date_day
    FROM {{ ref('dim_date') }}
    WHERE date_day BETWEEN (SELECT MIN(date_day) FROM snapshots)
                    AND (SELECT MAX(date_day) FROM snapshots)
),

game_days AS (
    SELECT
        app_id,
        date_day
    FROM first_observed
    JOIN spine
        ON date_day >= first_observed_day
),

final AS (
    SELECT
        gd.app_id,
        gd.date_day,
        a.avg_player_count,
        a.peak_player_count,
        COALESCE(a.observed_hours, 0) AS observed_hours
    FROM game_days gd
    LEFT JOIN aggregated a
        ON gd.app_id = a.app_id
        AND gd.date_day = a.date_day
)

SELECT *
FROM final

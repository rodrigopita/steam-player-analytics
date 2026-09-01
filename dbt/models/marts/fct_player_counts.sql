WITH
snapshots AS (
    SELECT *
    FROM {{ ref('stg_player_counts') }}
),

final AS (
    SELECT
        app_id,
        logical_hour::date AS date_day,
        logical_hour,
        EXTRACT(hour FROM logical_hour)::int AS hour_of_day,
        player_count
    FROM snapshots
)

SELECT *
FROM final

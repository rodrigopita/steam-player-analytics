SELECT
    app_id,
    logical_hour
FROM {{ ref('fct_player_counts') }}
GROUP BY app_id, logical_hour
HAVING COUNT(*) > 1

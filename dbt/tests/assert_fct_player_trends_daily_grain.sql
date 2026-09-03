SELECT
    app_id,
    date_day
FROM {{ ref('fct_player_trends_daily') }}
GROUP BY app_id, date_day
HAVING COUNT(*) > 1

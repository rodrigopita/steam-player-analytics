SELECT *
FROM {{ ref('fct_player_trends_daily') }}
WHERE avg_player_count_change IS DISTINCT FROM avg_player_count - avg_player_count_prev_day
    OR (
        avg_player_count_change_rate IS NULL
        AND avg_player_count IS NOT NULL
        AND avg_player_count_prev_day IS NOT NULL
        AND avg_player_count_prev_day != 0
    )
    OR (
        avg_player_count_change_rate IS NOT NULL
        AND (
            avg_player_count IS NULL
            OR avg_player_count_prev_day IS NULL
            OR avg_player_count_prev_day = 0
        )
    )
    OR observed_days_7d NOT BETWEEN 0 AND 7
    OR (avg_player_count_7d IS NULL) <> (observed_days_7d = 0)
    OR peak_player_count > peak_player_count_record
    OR last_record_day > date_day
    OR days_since_peak < 0

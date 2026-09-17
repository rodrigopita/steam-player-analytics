SELECT
    *
FROM {{ ref('fct_player_counts_daily') }}
WHERE observed_hours NOT BETWEEN 0 AND 24
    OR (observed_hours = 0 AND (avg_player_count IS NOT NULL OR peak_player_count IS NOT NULL))
    OR (observed_hours > 0 AND (avg_player_count IS NULL OR peak_player_count IS NULL))
    OR peak_player_count < avg_player_count

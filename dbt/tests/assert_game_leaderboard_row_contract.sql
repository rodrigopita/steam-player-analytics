SELECT *
FROM {{ ref('game_leaderboard') }}
WHERE current_player_count > peak_player_count_current_day
    OR peak_player_count_current_day > peak_player_count_record
    OR observed_hours_current_day NOT BETWEEN 1 AND 24

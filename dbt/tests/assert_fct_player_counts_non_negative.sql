SELECT *
FROM {{ ref('fct_player_counts') }}
WHERE player_count  < 0

WITH
ranked AS (
    SELECT
        'current_rank' AS rank_column,
        MAX(current_rank) AS max_rank,
        COUNT(DISTINCT current_rank) AS distinct_ranks
    FROM {{ ref('game_leaderboard') }}
    HAVING MAX(current_rank) != COUNT(DISTINCT current_rank)
),

ranked_7d AS (
    SELECT
        'rank_7d' AS rank_column,
        MAX(rank_7d) AS max_rank_7d,
        COUNT(DISTINCT rank_7d) AS distinct_ranks_7d
    FROM {{ ref('game_leaderboard') }}
    HAVING MAX(rank_7d) != COUNT(DISTINCT rank_7d)
)

SELECT *
FROM ranked
UNION ALL
SELECT *
FROM ranked_7d

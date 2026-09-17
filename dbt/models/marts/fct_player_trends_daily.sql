WITH
daily AS (
    SELECT *
    FROM {{ ref('fct_player_counts_daily') }}
),

compared AS (
    SELECT
        *,
        LAG(avg_player_count) OVER by_game AS avg_player_count_prev_day
    FROM daily
    WINDOW by_game AS (PARTITION BY app_id ORDER BY date_day)
),

smoothed AS (
    SELECT
        *,
        ROUND(
            AVG(avg_player_count) OVER (by_game ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1
        ) AS avg_player_count_7d,
        COUNT(avg_player_count) OVER(
            by_game ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::int AS observed_days_7d
    FROM compared
    WINDOW by_game AS (PARTITION BY app_id ORDER BY date_day)
),

records AS (
    SELECT
        *,
        MAX(peak_player_count) OVER (
            by_game ROWS UNBOUNDED PRECEDING
        ) AS peak_player_count_record
    FROM smoothed
    WINDOW by_game AS (PARTITION BY app_id ORDER BY date_day)
),

flagged AS (
    SELECT
        *,
        COALESCE(peak_player_count = peak_player_count_record, FALSE) AS is_peak_record
    FROM records
),

anchored AS (
    SELECT
        *,
        MAX(date_day) FILTER (WHERE is_peak_record) OVER (
            by_game ROWS UNBOUNDED PRECEDING
        ) AS last_record_day
    FROM flagged
    WINDOW by_game AS (PARTITION BY app_id ORDER BY date_day)
),

final AS (
    SELECT
        app_id,
        date_day,
        avg_player_count,
        peak_player_count,
        observed_hours,
        avg_player_count_prev_day,
        avg_player_count - avg_player_count_prev_day AS avg_player_count_change,
        ROUND(
            (avg_player_count - avg_player_count_prev_day)
                / NULLIF(avg_player_count_prev_day, 0), 4
        ) AS avg_player_count_change_rate,
        avg_player_count_7d,
        observed_days_7d,
        peak_player_count_record,
        date_day - last_record_day AS days_since_peak,
        last_record_day
    FROM anchored
)

SELECT *
FROM final

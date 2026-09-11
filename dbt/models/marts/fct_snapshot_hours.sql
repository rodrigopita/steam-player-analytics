{{ config(materialized='view') }}

WITH
runs AS (
    SELECT *
    FROM {{ ref('stg_snapshot_runs') }}
),

snapshots AS (
    SELECT *
    FROM {{ ref('stg_player_counts') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY logical_hour
            ORDER BY recorded_at DESC
        ) AS rn
    FROM runs
),

deduplicated AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
),

aggregated AS (
    SELECT
        logical_hour,
        COUNT(*) AS snapshot_rows
    FROM snapshots
    GROUP BY logical_hour
),

spine AS (
    SELECT generated_hour AS logical_hour
    FROM generate_series(
        (SELECT MIN(logical_hour) FROM snapshots),
        DATE_TRUNC('hour', NOW()),
        interval '1 hour'
    ) AS t(generated_hour)
),

joined AS (
    SELECT
        s.logical_hour,
        a.snapshot_rows,
        d.tracked_count,
        d.observed_count,
        d.unobservable_count,
        d.loaded_count,
        d.failed_count,
        d.latency_p50_ms,
        d.latency_max_ms,
        d.run_id,
        d.recorded_at
    FROM spine s
    LEFT JOIN aggregated a
        ON s.logical_hour = a.logical_hour
    LEFT JOIN deduplicated d
        ON s.logical_hour = d.logical_hour
),

classified AS (
    SELECT
        *,
        (snapshot_rows IS NOT NULL) AS has_observations,
        CASE
            WHEN (run_id IS NULL) AND (snapshot_rows IS NOT NULL) THEN 'unaudited'
            WHEN run_id IS NULL THEN 'unrecorded'
            WHEN (failed_count = 0)
                AND (loaded_count = observed_count)
                AND (observed_count > 0)
                THEN 'complete'
            ELSE 'ran_short'
        END AS status
    FROM joined
),

flagged AS (
    SELECT
        logical_hour,
        status,
        has_observations,
        logical_hour = MAX(logical_hour) FILTER(WHERE status = 'complete')
            OVER() AS is_high_water_mark,
        tracked_count,
        observed_count,
        unobservable_count,
        failed_count,
        loaded_count,
        snapshot_rows,
        latency_p50_ms,
        latency_max_ms,
        run_id,
        recorded_at
    FROM classified
)

SELECT *
FROM flagged

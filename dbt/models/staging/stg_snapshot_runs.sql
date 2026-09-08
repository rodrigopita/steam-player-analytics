WITH
source AS (
    SELECT *
    FROM {{ source('steam', 'snapshot_runs') }}
),

renamed AS (
    SELECT
        run_id,
        logical_hour,
        tracked_count,
        observed_count,
        unobservable_app_ids,
        loaded_count,
        latency_p50_ms,
        latency_max_ms,
        s3_key,
        recorded_at
    FROM source
),

counted AS (
    SELECT
        *,
        cardinality(unobservable_app_ids) AS unobservable_count,
        tracked_count - observed_count - cardinality(unobservable_app_ids) AS failed_count
    FROM renamed
)

SELECT *
FROM counted

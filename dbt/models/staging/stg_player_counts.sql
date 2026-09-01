WITH
source AS (
    SELECT *
    FROM {{ source('steam', 'player_counts') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY app_id, DATE_TRUNC('hour', logical_hour)
            ORDER BY logical_hour
        ) AS r_number
    FROM source
),

renamed AS (
    SELECT
        app_id,
        player_count,
        DATE_TRUNC('hour', logical_hour) AS logical_hour,
        s3_key,
        loaded_at,
        observed_at
    FROM deduplicated
    WHERE r_number = 1
)

SELECT *
FROM renamed

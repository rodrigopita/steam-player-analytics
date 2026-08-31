WITH
source AS (
    SELECT *
    FROM {{ source('steam', 'player_counts') }}
),

renamed AS (
    SELECT
        app_id,
        player_count,
        DATE_TRUNC('hour', logical_hour) AS logical_hour,
        s3_key,
        loaded_at,
        observed_at
    FROM source
)

SELECT *
FROM renamed

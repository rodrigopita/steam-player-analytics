WITH 
source AS (
    SELECT *
    FROM {{ source('steam', 'tracked_universe') }}
),

renamed AS (
    SELECT
        app_id,
        name AS app_name,
        source AS tracked_source,
        tracked_from,
        is_active
    FROM source
)

SELECT *
FROM renamed

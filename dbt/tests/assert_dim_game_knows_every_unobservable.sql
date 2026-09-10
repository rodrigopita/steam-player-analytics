{{ config(severity='warn') }}

WITH
runs AS (
    SELECT *
    FROM {{ ref('stg_snapshot_runs') }}
),

recent AS (
    SELECT *
    FROM runs
    WHERE unobservable_app_ids IS NOT NULL
    ORDER BY logical_hour DESC LIMIT 1
),

unobservable AS (
    SELECT
        app_id
    FROM recent, unnest(unobservable_app_ids) AS u(app_id)
)

SELECT
    u.app_id
FROM unobservable u
LEFT JOIN {{ ref('dim_game') }} g
    ON u.app_id = g.app_id
WHERE NOT is_unobservable OR is_unobservable IS NULL

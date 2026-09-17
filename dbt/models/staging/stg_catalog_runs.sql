WITH
source AS (
    SELECT *
    FROM {{ source('steam', 'catalog_runs') }}
),

renamed AS (
    SELECT
        run_id,
        logical_date,
        app_list_count,
        chart_count,
        skipped_chart_app_ids,
        newly_tracked_count,
        reactivated_count,
        deactivated_count,
        no_metadata_app_ids,
        metadata_upserted_count,
        app_list_key,
        most_played_key,
        recorded_at
    FROM source
),

counted AS (
    SELECT
        *,
        cardinality(skipped_chart_app_ids) AS skipped_chart_count,
        cardinality(no_metadata_app_ids) AS no_metadata_count
    FROM renamed
)

SELECT *
FROM counted

SELECT
    COUNT(*) AS actual_rows,
    (EXTRACT(epoch FROM MAX(logical_hour) - MIN(logical_hour)) / 3600 + 1)::int AS expected_rows,
    COUNT(*) FILTER (WHERE is_high_water_mark) AS flagged_rows
FROM {{ ref('fct_snapshot_hours') }}
HAVING COUNT(*) <> (EXTRACT(epoch FROM MAX(logical_hour) - MIN(logical_hour)) / 3600 + 1)::int
    OR COUNT(*) FILTER (WHERE is_high_water_mark) <> 1

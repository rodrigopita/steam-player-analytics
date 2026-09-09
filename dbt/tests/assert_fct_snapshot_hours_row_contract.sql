SELECT
    *
FROM {{ ref('fct_snapshot_hours') }}
WHERE status NOT IN ('unaudited', 'never_ran', 'complete', 'ran_short')
    OR (status = 'complete' AND snapshot_rows IS NULL)
    OR (status = 'never_ran' AND snapshot_rows IS NOT NULL)
    OR (status = 'unaudited' AND run_id IS NOT NULL)
    OR (status != 'complete' AND is_high_water_mark)

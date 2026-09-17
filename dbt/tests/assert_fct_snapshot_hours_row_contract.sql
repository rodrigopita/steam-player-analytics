SELECT
    *
FROM {{ ref('fct_snapshot_hours') }}
WHERE status NOT IN ('unaudited', 'unrecorded', 'complete', 'ran_short')
    OR (status = 'complete' AND snapshot_rows IS NULL)
    OR (status = 'unrecorded' AND snapshot_rows IS NOT NULL)
    OR (status = 'unaudited' AND run_id IS NOT NULL)
    OR (status != 'complete' AND is_high_water_mark)

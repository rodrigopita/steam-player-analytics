# cp4 failure demonstration, 2026-09-10

The cp4 done-criterion: a deliberately killed run can be rerun without
duplicates, and a query shows exactly which hours are missing and the
current high-water mark. This is the record of both, plus three real
failures from the night before that exercised the same machinery.

## The query

```sql
SELECT
    logical_hour,
    status,
    has_observations
FROM marts.fct_snapshot_hours
WHERE NOT has_observations
ORDER BY logical_hour;

SELECT logical_hour
FROM marts.fct_snapshot_hours
WHERE is_high_water_mark;
```

`fct_snapshot_hours` has one row per hour from the first observation to the
last dbt build. Status comes from two sides joined onto that spine: the
run's own audit row in `raw.snapshot_runs`, and a count of what
`stg_player_counts` holds for the hour. Unaudited means facts without an
audit row (every hour before 2026-09-07 04:00 UTC). Unrecorded means
neither. Ran short means an audit row that admits a failure, a short load,
or zero observations. Complete means none of that. The high-water mark is
the latest complete hour.

## Three real failures, 2026-09-09 17:00 to 2026-09-10 03:00 UTC

The machine was off from about 17:00 to 01:40. Eight hours, 17:00 through
00:00, have no row on either side and read as unrecorded. They stay that
way: catchup is off and the staleness guard refuses to snapshot an hour
more than two hours old, because a snapshot taken late would misrepresent
the hour it is filed under.

The 01:00 run fired at 01:43, before the warehouse container was
resolvable. The universe query failed, every fetch was upstream-failed,
and the bundle task crashed on a None universe. Two fixes came out of it.
The bundle task now skips when the universe is unavailable, since Steam
was never asked and there is nothing to land. And the status that was
called never_ran became unrecorded, because the warehouse cannot know
whether the scheduler fired or the run failed to reach it, and the audit
writer cannot record an outage of the database it writes to. Once the
warehouse was back, clearing the bundle task made the writer land the
honest row: run_id, every bundle column null, loaded_count 0. The hour
reads ran_short.

The 03:00 run failed because the warehouse password was changed on the
live database before Airflow was restarted with the new one. Cleared
whole at 03:05, inside the staleness window, it landed as a normal hour.

## The deliberate kill, 13:00 UTC

`load_raw_counts` was marked failed while the fetches were still running.
The bundle landed, the load never ran, and `record_run` on all_done wrote
the audit row. After `dbt build --select fct_snapshot_hours`:

    logical_hour            | status    | has_observations | observed | loaded | snapshot_rows
    2026-09-10 12:00:00+00  | complete  | t                | 105      | 105    | 105
    2026-09-10 13:00:00+00  | ran_short | f                | 105      | 0      |

    high-water mark: 2026-09-10 12:00:00+00
    raw rows for 13:00: 0

The audit row says what the bundle held and what the warehouse got, and
the difference is the failure.

Repair: clear `load_raw_counts` with downstream. The load inserted 105
rows (0 already present); `record_run` rewrote the same run_id at
13:02:36

    2026-09-10 13:00:00+00  | complete  | t                | 105      | 105    | 105

    high-water mark: 2026-09-10 13:00:00+00
    raw rows for 13:00: 105

Rerun: clear the same two tasks again. The load inserted 0 rows (105
already present); the audit row was rewritten at 13:05:11 and is still
the only one for the hour. Raw rows for the 13:00: 105. Full `dbt build`, 55
nodes green, including the grain test on `fct_player_counts`.

## What holds it together

The load runs its inserts in one transaction, so a kill mid-insert leaves
zero rows, never a partial hour. `ON CONFLICT (app_id, logical_hour) DO 
NOTHING` makes the rerun of a complete hour a no-op. The audit row is
keyed on run_id, which a cleared run keeps, and `ON CONFLICT DO UPDATE`
rewrites it rather than adding a second. `loaded_count` is counted from
the table when the row is written, not taken from the load task, so the
row describes durable state and a rerun recomputes the same number.

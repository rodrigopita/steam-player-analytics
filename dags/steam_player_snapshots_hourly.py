import logging
import statistics
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from include import object_store, steam_api

logger = logging.getLogger(__name__)


@dag(
    schedule="@hourly",
    catchup=False,  # snapshots are unrecoverable: a past hour can never be validly backfilled
    description=(
        "Hourly player-count snapshots for the tracked universe: fetches current players "
        "per game from the Steam API, lands one raw JSON bundle per run in S3, and loads "
        "the warehouse raw table idempotently. Missed hours stay missing by design."
    ),
    tags=["steam", "snapshots", "ingestion"],
)
def steam_player_snapshots_hourly():

    @task
    def get_tracked_app_ids() -> list[int]:
        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT app_id FROM raw.tracked_universe WHERE is_active ORDER BY app_id")
            ids = [row[0] for row in cur.fetchall()]
        if not ids:
            # fail loud rather than fan out over nothing and land empty-but-green bundles
            raise RuntimeError(
                "Tracked universe is empty - run steam_catalog_refresh before this DAG"
            )
        logger.info(f"Snapshotting {len(ids)} tracked games")
        return ids

    @task(retries=3, retry_exponential_backoff=True)
    def fetch_player_count(app_id: int, logical_date: datetime | None = None) -> dict:
        staleness = datetime.now(UTC) - logical_date
        if staleness > timedelta(hours=2):
            raise AirflowSkipException(
                f"Logical hour is {staleness} old; a snapshot now would misrepresent it. "
                "This hour is permanently missing."
            )
        started = time.monotonic()
        response = steam_api.get_player_count(app_id)
        if response is None:
            logger.info(
                f"Steam exposes no public player stats for {app_id} (endpoint 404); "
                "unobservable, not a failure"
            )
            return {
                "app_id": app_id,
                "logical_date": logical_date.isoformat(),
                "observed_at": datetime.now(UTC).isoformat(),
                "latency_ms": round((time.monotonic() - started) * 1000),
                "status": "unobservable",
            }
        return {
            "app_id": app_id,
            "player_count": response[
                "player_count"
            ],  # KeyError on a malformed response: fail and retry, never record 0
            "api_result": response.get("result"),
            "logical_date": logical_date.isoformat(),
            "observed_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((time.monotonic() - started) * 1000),
            "status": "observed",
        }

    # all_done: one failed game must not sink the hour for the rest. Unobservable games now arrive
    # as rows, so tracked_count minus observed minus unobservable is the count of failures.
    @task(trigger_rule="all_done")
    def load_to_s3(
        rows: Sequence[dict], app_ids: Sequence[int], logical_date: datetime | None = None
    ) -> str:
        results = list(rows)
        observed, unobservable = [], []
        for row in results:
            if row["status"] == "observed":
                observed.append(row)
            else:
                unobservable.append(row)
        logger.info(
            f"Bundling {len(observed)} observed and {len(unobservable)} unobservable "
            f"of {len(app_ids)} tracked games for {logical_date:%Y-%m-%d %H:00}"
        )
        return object_store.write_json(
            key=object_store.player_counts_key(logical_date),
            payload={
                "logical_date": logical_date.isoformat(),
                "tracked_count": len(app_ids),
                "observed": observed,
                "unobservable": unobservable,
            },
        )

    @task
    def load_raw_counts(key: str) -> int:
        payload = object_store.read_json(key)
        rows = [
            # .get for observed_at: bundles landed before the field existed load as NULL
            (obs["app_id"], obs["player_count"], obs["logical_date"], obs.get("observed_at"), key)
            for obs in payload["observed"]
        ]

        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        with conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw.player_counts
                    (app_id, player_count, logical_hour, observed_at, s3_key)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (app_id, logical_hour) DO NOTHING
                """,
                rows,
            )
            inserted = cur.rowcount
            logger.info(
                f"Inserted {inserted} rows from {key} ({len(rows) - inserted} already present)"
            )
        return inserted

    @task(trigger_rule="all_done")
    def record_run(
        key: str | None, logical_date: datetime | None = None, run_id: str | None = None
    ) -> None:
        tracked_count = None
        observed_count = unobservable_app_ids = latency_p50_ms = latency_max_ms = None

        if key:
            payload = object_store.read_json(key)
            tracked_count = payload.get("tracked_count")
            observed = payload.get("observed")
            unobservable = payload.get("unobservable")

            observed_count = len(observed)
            unobservable_app_ids = (
                [row["app_id"] for row in unobservable] if unobservable is not None else None
            )
            latencies = [row["latency_ms"] for row in observed]
            if latencies:
                latency_p50_ms = round(statistics.median(latencies))
                latency_max_ms = max(latencies)

        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw.player_counts WHERE logical_hour = %s", (logical_date,)
            )
            loaded_count = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO raw.snapshot_runs
                    (run_id, logical_hour, tracked_count, observed_count, unobservable_app_ids,
                        loaded_count, latency_p50_ms, latency_max_ms, s3_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    logical_hour = EXCLUDED.logical_hour,
                    tracked_count = EXCLUDED.tracked_count,
                    observed_count = EXCLUDED.observed_count,
                    unobservable_app_ids = EXCLUDED.unobservable_app_ids,
                    loaded_count = EXCLUDED.loaded_count,
                    latency_p50_ms = EXCLUDED.latency_p50_ms,
                    latency_max_ms = EXCLUDED.latency_max_ms,
                    s3_key = EXCLUDED.s3_key,
                    recorded_at = now()
                """,
                (
                    run_id,
                    logical_date,
                    tracked_count,
                    observed_count,
                    unobservable_app_ids,
                    loaded_count,
                    latency_p50_ms,
                    latency_max_ms,
                    key,
                ),
            )
        unobservable_count = len(unobservable_app_ids) if unobservable_app_ids is not None else None
        logger.info(
            f"Recorded run {run_id}: {observed_count} observed, {unobservable_count} unobservable, "
            f"{loaded_count} loaded of {tracked_count} tracked for {logical_date:%Y-%m-%d %H:00}"
        )

    ids = get_tracked_app_ids()
    rows = fetch_player_count.expand(app_id=ids)
    key = load_to_s3(rows=rows, app_ids=ids)
    loaded = load_raw_counts(key)
    loaded >> record_run(key=key)


steam_player_snapshots_hourly()

import logging
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
            cur.execute("SELECT app_id FROM tracked_universe WHERE is_active ORDER BY app_id")
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
        return {
            "app_id": app_id,
            "player_count": response[
                "player_count"
            ],  # KeyError on a malformed response: fail and retry, never record 0
            "api_result": response.get("result"),
            "logical_date": logical_date.isoformat(),
            "observed_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((time.monotonic() - started) * 1000),
        }

    @task
    def load_to_s3(
        rows: Sequence[dict], app_ids: Sequence[int], logical_date: datetime | None = None
    ) -> str:
        observed = list(rows)
        logger.info(
            f"Bundling {len(observed)} of {len(app_ids)} tracked games for {logical_date:%Y-%m-%d %H:00}"
        )
        return object_store.write_json(
            key=object_store.player_counts_key(logical_date),
            payload={
                "logical_date": logical_date.isoformat(),
                "tracked_count": len(app_ids),
                "observed": observed,
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_player_counts (
                    app_id        bigint      NOT NULL,
                    player_count  integer     NOT NULL,
                    logical_hour  timestamptz NOT NULL,
                    observed_at   timestamptz,
                    s3_key        text        NOT NULL,
                    loaded_at     timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (app_id, logical_hour)
                )
                """)
            cur.executemany(
                """
                INSERT INTO raw_player_counts (app_id, player_count, logical_hour, observed_at, s3_key)
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

    ids = get_tracked_app_ids()
    rows = fetch_player_count.expand(app_id=ids)
    key = load_to_s3(rows=rows, app_ids=ids)
    load_raw_counts(key)


steam_player_snapshots_hourly()

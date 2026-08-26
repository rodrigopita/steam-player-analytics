from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from include import object_store, steam_api

TRACKED_APP_IDS = [
    730,  # CS:GO
    440,  # Team Fortress 2
    570,  # Dota 2
    578080,  # PUBG
    252490,  # Rust
    304930,  # Unturned
    3240220,  # Grand Theft Auto V
    1174180,  # Apex Legends
    1091500,  # Cyberpunk 2077
    945360,  # Among Us
    359550,  # Tom Clancy's Rainbow Six Siege
    252950,  # Rocket League
]


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
    def get_tracked_app_ids():
        return (
            TRACKED_APP_IDS  # cp2: replaced by SELECT app_id FROM trakced_universe WHERE is_active
        )

    @task(retries=3, retry_exponential_backoff=True)
    def fetch_player_count(app_id: int, logical_date: datetime | None = None) -> dict:
        staleness = datetime.now(UTC) - logical_date
        if staleness > timedelta(hours=2):
            raise AirflowSkipException(
                f"Logical hour is {staleness} old; a snapshot now would misrepresent it. "
                "This hour is permanently missing."
            )
        return {
            "app_id": app_id,
            "player_count": steam_api.get_player_count(app_id),
            "logical_date": logical_date.isoformat(),
        }

    @task
    def load_to_s3(rows: Sequence[dict], logical_date: datetime | None = None) -> str:
        return object_store.write_json(
            key=object_store.player_counts_key(logical_date),
            payload={"observed": list(rows)},
        )

    @task
    def load_raw_counts(key: str) -> int:
        payload = object_store.read_json(key)
        rows = [
            (obs["app_id"], obs["player_count"], obs["logical_date"], key)
            for obs in payload["observed"]
        ]

        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_player_counts (
                    app_id        bigint      NOT NULL,
                    player_count  integer     NOT NULL,
                    logical_hour  timestamptz NOT NULL,
                    s3_key        text        NOT NULL,
                    loaded_at     timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (app_id, logical_hour)
                )
                """)
            cur.executemany(
                """
                INSERT INTO raw_player_counts (app_id, player_count, logical_hour, s3_key)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (app_id, logical_hour) DO NOTHING
                """,
                rows,
            )
            inserted = cur.rowcount
        return inserted

    ids = get_tracked_app_ids()
    rows = fetch_player_count.expand(app_id=ids)
    key = load_to_s3(rows)
    load_raw_counts(key)


steam_player_snapshots_hourly()

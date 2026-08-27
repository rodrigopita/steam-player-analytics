import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from psycopg.types.json import Jsonb

from include import object_store, steam_api

logger = logging.getLogger(__name__)

# Curated half of the universe rule: tracked = accumulated chart top-100 UNION this seed.
# Lives in code, not only in the table, so a rebuilt warehouse reproduces the same universe.
SEED_APP_IDS = [
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
    271590,  # Grand Theft Auto V Legacy (delisted from sale, still heavily played)
]


@dag(
    schedule="@daily",
    catchup=False,  # state pipeline: a missed day is simply covered by the next run
    description=(
        "Maintains the tracked universe and game metadata: discovers the full Steam games "
        "catalog, merges the daily most-played top-100 with the curated seed into "
        "tracked_universe (add-never-remove), and refreshes app_metadata from the storefront "
        "appdetails endpoint through the steam_metadata pool."
    ),
    tags=["steam", "catalog", "metadata"],
)
def steam_catalog_refresh():

    @task(retries=3, retry_exponential_backoff=True)
    def discover_apps(logical_date: datetime | None = None) -> str:
        logical_date = logical_date or datetime.now(UTC)
        apps = steam_api.get_app_list()
        logger.info(f"Discovered {len(apps)} games in the Steam catalog")
        return object_store.write_json(
            key=object_store.app_list_key(logical_date),
            payload={
                "logical_date": logical_date.isoformat(),
                "fetched_at": datetime.now(UTC).isoformat(),
                "app_count": len(apps),
                "apps": apps,
            },
        )

    @task(retries=3, retry_exponential_backoff=True)
    def fetch_most_played(logical_date: datetime | None = None) -> str:
        logical_date = logical_date or datetime.now(UTC)
        most_played = steam_api.get_most_played()
        logger.info(f"Fetched {len(most_played)} most played games from the Steam API")
        return object_store.write_json(
            key=object_store.most_played_key(logical_date),
            payload={
                "logical_date": logical_date.isoformat(),
                "fetched_at": datetime.now(UTC).isoformat(),
                "most_played_count": len(most_played),
                "most_played": most_played,
            },
        )

    @task
    def filter_tracked_universe_ids(app_list_key: str, most_played_key: str) -> list[int]:
        app_list = object_store.read_json(app_list_key)
        most_played = object_store.read_json(most_played_key)
        names = {app["appid"]: app["name"] for app in app_list["apps"]}

        # seed listed first: on a seed∩chart collision, source attribution goes to 'seed'
        candidates = [(app_id, "seed") for app_id in SEED_APP_IDS]
        candidates += [(entry["appid"], "chart") for entry in most_played["most_played"]]

        rows, unknown = [], []
        for app_id, source in candidates:
            if app_id in names:
                rows.append((app_id, names[app_id], source))
            elif source == "seed":
                # curation vouches for seed games the storefront no longer lists
                # (e.g. delisted but alive); dim_game gets the real name from appdetails
                rows.append((app_id, "(not in app list)", source))
            else:
                unknown.append(app_id)
        if unknown:
            logger.warning(f"Skipping {len(unknown)} appids missing from the app list: {unknown}")

        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tracked_universe (
                    app_id       bigint      PRIMARY KEY,
                    name         text        NOT NULL,
                    source       text        NOT NULL,
                    tracked_from timestamptz NOT NULL DEFAULT now(),
                    is_active    boolean     NOT NULL DEFAULT true
                )
                """)
            cur.executemany(
                """
                INSERT INTO tracked_universe (app_id, name, source)
                VALUES (%s, %s, %s)
                ON CONFLICT (app_id) DO NOTHING
                """,
                rows,
            )
            newly_tracked = cur.rowcount

            # chart presence is evidence of life: undo a delisting-based deactivation
            chart_ids = [entry["appid"] for entry in most_played["most_played"]]
            cur.execute(
                "UPDATE tracked_universe SET is_active = true WHERE NOT is_active AND app_id = ANY(%s)",
                (chart_ids,),
            )
            if cur.rowcount:
                logger.warning(f"Reactivated {cur.rowcount} games on chart-presence evidence")

            cur.execute("SELECT app_id FROM tracked_universe WHERE is_active ORDER BY app_id")
            ids = [row[0] for row in cur.fetchall()]

        logger.info(f"Universe: {len(ids)} tracked games ({newly_tracked} new this run)")
        return ids

    @task(pool="steam_metadata", retries=3, retry_exponential_backoff=True)
    def fetch_app_details(app_id: int, logical_date: datetime | None = None) -> dict:
        logical_date = logical_date or datetime.now(UTC)
        started = time.monotonic()
        envelope = steam_api.get_app_details(app_id)
        object_store.write_json(
            key=object_store.app_details_key(logical_date, app_id),
            payload={
                "app_id": app_id,
                "logical_date": logical_date.isoformat(),
                "observed_at": datetime.now(UTC).isoformat(),
                "latency_ms": round((time.monotonic() - started) * 1000),
                "envelope": envelope,
            },
        )
        data = steam_api.extract_app_details(envelope, app_id)
        if data is None:
            logger.warning(f"No appdetails data for {app_id} - candidate for deactivation")
            return {"app_id": app_id, "success": False}
        return {"app_id": app_id, "success": True, "data": data}

    @task
    def upsert_app_metadata(all_details: Sequence[dict]) -> int:
        results = list(all_details)
        succeeded = [r for r in results if r["success"]]
        failed_ids = [r["app_id"] for r in results if not r["success"]]

        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_metadata (
                    app_id          bigint      PRIMARY KEY,
                    data            jsonb       NOT NULL,
                    first_loaded_at timestamptz NOT NULL DEFAULT now(),
                    updated_at      timestamptz NOT NULL DEFAULT now()
                )
                """)
            cur.executemany(
                """
                INSERT INTO app_metadata (app_id, data)
                VALUES (%s, %s)
                ON CONFLICT (app_id) DO UPDATE
                    SET data = EXCLUDED.data, updated_at = now()
                """,
                [(r["app_id"], Jsonb(r["data"])) for r in succeeded],
            )
            if failed_ids:
                cur.execute(
                    "UPDATE tracked_universe SET is_active = false WHERE is_active AND app_id = ANY(%s)",
                    (failed_ids,),
                )
                logger.warning(
                    f"Deactivated {cur.rowcount} games with no appdetails data: {failed_ids}"
                )
        logger.info(f"Upserted metadata for {len(succeeded)} games")
        return len(succeeded)

    app_list_key = discover_apps()
    most_played_key = fetch_most_played()
    ids = filter_tracked_universe_ids(app_list_key=app_list_key, most_played_key=most_played_key)
    details = fetch_app_details.expand(app_id=ids)
    upsert_app_metadata(details)


steam_catalog_refresh()

import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime

from airflow.sdk import dag, task
from airflow.sdk.types import RuntimeTaskInstanceProtocol
from psycopg.types.json import Jsonb

from include import object_store, steam_api, warehouse

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
    def discover_apps(logical_date: datetime) -> str:
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
    def fetch_most_played(logical_date: datetime) -> str:
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
    def filter_tracked_universe_ids(
        app_list_key: str, most_played_key: str, ti: RuntimeTaskInstanceProtocol
    ) -> list[int]:
        app_list = object_store.read_json(app_list_key)
        most_played = object_store.read_json(most_played_key)
        names = {app["appid"]: app["name"] for app in app_list["apps"]}

        # seed first, a seed id's chart copy is dropped so attribution stays with 'seed'
        candidates = [(app_id, "seed") for app_id in SEED_APP_IDS]
        candidates += [
            (entry["appid"], "chart")
            for entry in most_played["most_played"]
            if entry["appid"] not in SEED_APP_IDS
        ]

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
            logger.info(f"Skipping {len(unknown)} appids missing from the app list: {unknown}")

        conn = warehouse.connect()
        with conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw.tracked_universe (app_id, name, source)
                VALUES (%s, %s, %s)
                ON CONFLICT (app_id) DO NOTHING
                """,
                rows,
            )
            newly_tracked = cur.rowcount

            # chart presence is evidence of life: undo a delisting-based deactivation
            chart_ids = [entry["appid"] for entry in most_played["most_played"]]
            cur.execute(
                "UPDATE raw.tracked_universe SET is_active = true "
                "WHERE NOT is_active AND app_id = ANY(%s)",
                (chart_ids,),
            )
            if reactivated_count := cur.rowcount:
                logger.warning(f"Reactivated {reactivated_count} games on chart-presence evidence")

            cur.execute("SELECT app_id FROM raw.tracked_universe WHERE is_active ORDER BY app_id")
            ids = [row[0] for row in cur.fetchall()]

        logger.info(f"Universe: {len(ids)} tracked games ({newly_tracked} new this run)")

        ti.xcom_push(
            key="audit",
            value={
                "app_list_count": len(names),
                "chart_count": len(chart_ids),
                "skipped_chart_app_ids": unknown,
                "newly_tracked_count": newly_tracked,
                "reactivated_count": reactivated_count,
            },
        )
        return ids

    @task(pool="steam_metadata", retries=3, retry_exponential_backoff=True)
    def fetch_app_details(app_id: int, logical_date: datetime) -> dict:
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

    @task(multiple_outputs=True)
    def upsert_app_metadata(all_details: Sequence[dict], most_played_key: str) -> dict:
        results = list(all_details)
        succeeded = [r for r in results if r["success"]]
        failed_ids = [r["app_id"] for r in results if not r["success"]]

        # chart presence outranks missing metadata: a region-locked game
        # (e.g. MahjongSoul under cc=us) has no US appdetails but is alive
        chart_ids = {e["appid"] for e in object_store.read_json(most_played_key)["most_played"]}
        to_deactivate = [i for i in failed_ids if i not in chart_ids]
        still_charting = [i for i in failed_ids if i in chart_ids]
        if still_charting:
            logger.warning(
                "No appdetails data but still charting - keeping active, no metadata: "
                f"{still_charting}"
            )

        conn = warehouse.connect()
        with conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw.app_metadata (app_id, data)
                VALUES (%s, %s)
                ON CONFLICT (app_id) DO UPDATE
                    SET data = EXCLUDED.data, updated_at = now()
                """,
                [(r["app_id"], Jsonb(r["data"])) for r in succeeded],
            )
            deactivated_count = 0
            if to_deactivate:
                cur.execute(
                    "UPDATE raw.tracked_universe SET is_active = false "
                    "WHERE is_active AND app_id = ANY(%s)",
                    (to_deactivate,),
                )
                deactivated_count = cur.rowcount
                logger.warning(
                    f"Deactivated {deactivated_count} games with no appdetails data: {to_deactivate}"
                )
        logger.info(f"Upserted metadata for {len(succeeded)} games")
        return {
            "metadata_upserted_count": len(succeeded),
            "no_metadata_app_ids": still_charting,
            "deactivated_count": deactivated_count,
        }

    @task(trigger_rule="all_done")
    def record_run(
        app_list_key: str | None,
        most_played_key: str | None,
        universe: dict | None,
        metadata: dict | None,
        logical_date: datetime,
        run_id: str,
    ) -> None:
        universe = universe or {}
        metadata = metadata or {}

        conn = warehouse.connect()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.catalog_runs
                    (run_id, logical_date, app_list_count, chart_count, skipped_chart_app_ids,
                        newly_tracked_count, reactivated_count, deactivated_count,
                        no_metadata_app_ids, metadata_upserted_count, app_list_key,
                        most_played_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    logical_date = EXCLUDED.logical_date,
                    app_list_count = EXCLUDED.app_list_count,
                    chart_count = EXCLUDED.chart_count,
                    skipped_chart_app_ids = EXCLUDED.skipped_chart_app_ids,
                    newly_tracked_count = COALESCE(
                        raw.catalog_runs.newly_tracked_count, EXCLUDED.newly_tracked_count
                    ),
                    reactivated_count = COALESCE(
                        raw.catalog_runs.reactivated_count, EXCLUDED.reactivated_count
                    ),
                    deactivated_count = COALESCE(
                        raw.catalog_runs.deactivated_count, EXCLUDED.deactivated_count
                    ),
                    no_metadata_app_ids = EXCLUDED.no_metadata_app_ids,
                    metadata_upserted_count = EXCLUDED.metadata_upserted_count,
                    app_list_key = EXCLUDED.app_list_key,
                    most_played_key = EXCLUDED.most_played_key,
                    recorded_at = now()
                """,
                (
                    run_id,
                    logical_date,
                    universe.get("app_list_count"),
                    universe.get("chart_count"),
                    universe.get("skipped_chart_app_ids"),
                    universe.get("newly_tracked_count"),
                    universe.get("reactivated_count"),
                    metadata.get("deactivated_count"),
                    metadata.get("no_metadata_app_ids"),
                    metadata.get("metadata_upserted_count"),
                    app_list_key,
                    most_played_key,
                ),
            )
        skipped = universe.get("skipped_chart_app_ids")
        skipped_count = len(skipped) if skipped is not None else None
        logger.info(
            f"Recorded run {run_id}: {universe.get('newly_tracked_count')} new, "
            f"{universe.get('reactivated_count')} reactivated, "
            f"{metadata.get('deactivated_count')} deactivated, "
            f"{skipped_count} chart ids skipped for {logical_date:%Y-%m-%d}"
        )

    app_list_key = discover_apps()
    most_played_key = fetch_most_played()
    universe = filter_tracked_universe_ids(
        app_list_key=app_list_key, most_played_key=most_played_key
    )
    details = fetch_app_details.expand(app_id=universe)
    metadata = upsert_app_metadata(all_details=details, most_played_key=most_played_key)
    record_run(
        app_list_key=app_list_key,
        most_played_key=most_played_key,
        universe=universe["audit"],
        metadata=metadata,
    )


steam_catalog_refresh()

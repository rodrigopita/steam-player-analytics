import logging
from datetime import UTC, datetime

from airflow.sdk import dag, task

from include import object_store, steam_api

logger = logging.getLogger(__name__)


@dag(
    schedule="@daily",
    description="Refreshes the Steam catalog by fetching the latest data from the Steam API.",
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
    def filter_tracked_universe_ids(s3_key_1, s3_key_2):
        pass

    @task
    def fetch_app_details(app_id):
        pass

    @task
    def upsert_app_metadata(all_details):
        pass

    key_1 = discover_apps()
    key_2 = fetch_most_played()
    ids = filter_tracked_universe_ids(key_1, key_2)
    details = fetch_app_details.expand(app_id=ids)
    upsert_app_metadata(details)


steam_catalog_refresh()

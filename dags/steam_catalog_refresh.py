import requests
from airflow.models import Variable
from airflow.sdk import dag, task

STEAM_API_KEY = Variable.get("steam_api_key")
STEAM_BASE_URL = "https://api.steampowered.com"
MAX_RESULTS = 50_000


@dag(
    schedule="@daily",
    description="Refreshes the Steam catalog by fetching the latest data from the Steam API.",
    tags=["steam", "catalog", "metadata"],
)
def steam_catalog_refresh():

    @task
    def discover_apps():
        url = f"{STEAM_BASE_URL}/IStoreService/GetAppList/v1"
        params = {
            "key": STEAM_API_KEY,
            "include_games": "true",
            "include_dlc": "false",
            "include_software": "false",
            "include_videos": "false",
            "include_hardware": "false",
            "max_results": MAX_RESULTS,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @task
    def fetch_most_played():
        pass

    @task
    def filter_tracked_universe_ids(s3_key_1, s3_key_2):
        pass

    @task
    def fetch_app_details(app_ids):
        pass

    @task
    def upsert_app_metadata(all_details):
        pass

    key_1 = discover_apps()
    key_2 = fetch_most_played()
    ids = filter_tracked_universe_ids(key_1, key_2)
    details = fetch_app_details.expand(app_ids=ids)
    upsert_app_metadata(details)


steam_catalog_refresh()

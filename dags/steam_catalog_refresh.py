import requests
from airflow.models import Variable
from airflow.sdk import dag, task

STEAM_API_KEY = Variable.get("steam_api_key")
STEAM_BASE_URL = "https://api.steampowered.com"


@dag(
    schedule="@daily",
    description="Refreshes the Steam catalog by fetching the latest data from the Steam API.",
    tags=["steam", "catalog", "metadata"],
)
def steam_catalog_refresh():

    @task
    def fetch_steam_catalog():
        url = f"{STEAM_BASE_URL}/IStoreService/GetAppList/v1"
        params = {
            "key": STEAM_API_KEY,
            "include_dlc": "false",
            "max_results": 10000,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    fetch_steam_catalog()

steam_catalog_refresh()

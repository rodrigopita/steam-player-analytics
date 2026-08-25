import requests
from airflow.sdk import dag, task

STEAM_BASE_URL = "https://api.steampowered.com"
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
    description="Fetches hourly snapshots of Steam player data from the Steam API.",
    tags=["steam", "catalog", "metadata"],
)
def steam_player_snapshots_hourly():

    @task
    def get_tracked_app_ids():
        return TRACKED_APP_IDS  # cp2: replaced by SELECT app_id FROM trakced_universe WHERE is_active

    @task(retries=3, retry_exponential_backoff=True)
    def fetch_player_count(app_id: int, logical_date=None) -> dict:
        url = f"{STEAM_BASE_URL}/ISteamUserStats/GetNumberOfCurrentPlayers/v1"
        params = {"appid": app_id}
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return {
            "app_id": app_id,
            "player_count": data.get("response", {}).get("player_count", 0),
            "logical_date": logical_date,
        }

    @task
    def load_raw_counts(rows: list[dict]) -> None:
        pass

    ids = get_tracked_app_ids()
    rows = fetch_player_count.expand(app_id=ids)
    load_raw_counts(rows)

steam_player_snapshots_hourly()

import requests
from airflow.sdk import dag, task


@dag(
    schedule="@hourly",
    description="Fetches hourly snapshots of Steam player data from the Steam API.",
    tags=["steam", "catalog", "metadata"],
)
def steam_player_snapshots_hourly():
    pass


steam_player_snapshots_hourly()

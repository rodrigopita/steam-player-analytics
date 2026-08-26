"""Steam API client.

Two distinct hosts, deliberately modeled as such:

- WEB_API_URL: the official Steam Web API (documented, keyed where noted).
- STORE_API_URL: the storefront's own endpoint (keyless, undocumented,
  aggressively throttled — callers must pace themselves; in Airflow that
  means the steam_metadata pool on the calling task).

Endpoints, params, and response shapes live here. Retries, pacing, and
pipeline policy (staleness guards, what to do with delisted apps) belong
to the calling task, not this module.
"""

import requests
from airflow.sdk import Variable

WEB_API_URL = "https://api.steampowered.com"
STORE_API_URL = "https://store.steampowered.com"
TIMEOUT = 30
APP_LIST_PAGE_SIZE = 50_000


def _get(url: str, params: dict) -> dict:
    response = requests.get(url, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def get_player_count(app_id: int) -> int:
    """Current concurrent players for one app. No key required."""
    data = _get(f"{WEB_API_URL}/ISteamUserStats/GetNumberOfCurrentPlayers/v1", {"appid": app_id})
    return data.get("response", {}).get("player_count", 0)


def get_app_list() -> list[dict]:
    """
    The full Steam games catalog (appid, name, last_modified,
    price_change_number), games only. Paginates until exhausted —
    expect a handful of requests and 100k+ entries.
    """
    params = {
        "key": Variable.get("steam_api_key"),
        "include_games": "true",
        "include_dlc": "false",
        "include_software": "false",
        "include_videos": "false",
        "include_hardware": "false",
        "max_results": APP_LIST_PAGE_SIZE,
    }
    apps: list[dict] = []
    while True:
        data = _get(f"{WEB_API_URL}/IStoreService/GetAppList/v1", params).get("response", {})
        apps.extend(data.get("apps", []))
        if not data.get("have_more_results"):
            return apps
        params["last_appid"] = data["last_appid"]


def get_most_played() -> list[dict]:
    """
    Steam's most-played chart: ~top 100 by concurrent players, ranked.
    Endpoint is used by the storefront charts page but served from the
    Web API host; undocumented, so treat the shape as observed, not
    contractual. No key required.
    """
    data = _get(f"{WEB_API_URL}/ISteamChartsService/GetMostPlayedGames/v1", {})
    return data.get("response", {}).get("ranks", [])


def get_app_details(app_id: int) -> dict:
    """
    Raw appdetails envelope for one app, as the storefront returned it:
    {"<app_id>": {"success": bool, "data": {...}}}. Land this verbatim;
    unwrap with extract_app_details for the parts worth modeling.

    cc (country code) selects the storefront region: currency, prices,
    and regional availability. l (language) selects the text fields:
    description, genres, etc. Both are pinned so responses are
    deterministic instead of varying with the caller's IP location.
    """
    return _get(f"{STORE_API_URL}/api/appdetails", {"appids": app_id, "cc": "us", "l": "english"})


def extract_app_details(envelope: dict, app_id: int) -> dict | None:
    """The data block from an appdetails envelope, or None when Steam
    reports no data (delisted, region-locked, not actually a game)."""
    entry = envelope.get(str(app_id), {})
    return entry.get("data") if entry.get("success") else None

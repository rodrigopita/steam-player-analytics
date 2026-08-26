"""Raw-zone object store access.

Every read and write of the raw zone goes through this module, so the
storage provider, bucket name, connection id, and key layout each have
exactly one definition. Swapping S3 for another store, or renaming the
bucket, must never require touching a DAG file.
"""

import json
from datetime import datetime
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_BUCKET = "steam-player-analytics-raw-rp"
AWS_CONN_ID = "aws_default"


def player_counts_key(logical_date: datetime) -> str:
    """
    Key for one hourly snapshot bundle. Deterministic per logical hour,
    so retries and cleared runs overwrite instead of duplicating.
    """
    return f"raw/player-counts/dt={logical_date:%Y-%m-%d}/{logical_date:%H}-snapshot.json"


def app_list_key(logical_date: datetime) -> str:
    return f"raw/app-list/dt={logical_date:%Y-%m-%d}/app-list.json"


def most_played_key(logical_date: datetime) -> str:
    return f"raw/most-played/dt={logical_date:%Y-%m-%d}/most-played.json"


def app_details_key(logical_date: datetime, app_id: int) -> str:
    return f"raw/app-details/dt={logical_date:%Y-%m-%d}/{app_id}.json"


def write_json(key: str, payload: Any) -> str:
    """
    Serialize payload and land it at key. Overwrites: safe to rerun, since the
    caller's key is deterministic, with bucket versioning as the safety net.
    """
    S3Hook(aws_conn_id=AWS_CONN_ID).load_string(
        string_data=json.dumps(payload, default=str),
        key=key,
        bucket_name=RAW_BUCKET,
        replace=True,
    )
    return key


def read_json(key: str) -> Any:
    return json.loads(S3Hook(aws_conn_id=AWS_CONN_ID).read_key(key=key, bucket_name=RAW_BUCKET))

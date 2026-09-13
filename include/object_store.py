"""Raw-zone object store access.

Every read and write of the raw zone goes through this module, so the
storage provider, connection id, and key layout each have exactly one
definition, and the bucket name has exactly one read: the raw_bucket
Airflow Variable, set per deployment. Swapping S3 for another store, or
pointing at another bucket, must never require touching a DAG file.
"""

import json
from datetime import datetime
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable

AWS_CONN_ID = "aws_default"


def raw_bucket() -> str:
    """
    Bucket name from the raw_bucket Variable (AIRFLOW_VAR_RAW_BUCKET in .env).
    Read at call time so DAG parsing never depends on it. No default: a missing
    value fails the first S3 call naming the variable, instead of AccessDenied
    against a bucket that is not this deployment's.
    """
    return Variable.get("raw_bucket")


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
        bucket_name=raw_bucket(),
        replace=True,
    )
    return key


def read_json(key: str) -> Any:
    return json.loads(S3Hook(aws_conn_id=AWS_CONN_ID).read_key(key=key, bucket_name=raw_bucket()))

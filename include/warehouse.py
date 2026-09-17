"""Warehouse connection.

Every connection to the warehouse goes through this module, so the
Airflow connection id and the driver each have exactly one definition.
Renaming the connection, or moving the warehouse, must never require
touching a DAG file. Airflow writes only to the `raw` schema through
it; dbt owns everything else.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook

WAREHOUSE_CONN_ID = "warehouse"


def connect():
    return PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()

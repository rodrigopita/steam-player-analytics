"""The dashboard's one definition of the warehouse connection.

Same defaults as dbt/profiles.yml: the compose password unless
DBT_WAREHOUSE_PASSWORD is exported in the shell, which only matters if the
compose password was changed. SQL decides the numbers; cells only plot.
"""

import os

import pandas as pd
from sqlalchemy import URL, create_engine

engine = create_engine(
    URL.create(
        "postgresql+psycopg",
        username="warehouse",
        password=os.environ.get("DBT_WAREHOUSE_PASSWORD", "steamdw-local"),
        host="localhost",
        port=5433,
        database="steam",
    )
)


def q(sql: str) -> pd.DataFrame:
    """Run one SELECT against the marts and return the frame."""
    return pd.read_sql(sql, engine)

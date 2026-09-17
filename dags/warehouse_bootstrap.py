import logging
from pathlib import Path

from airflow.sdk import dag, task

from include import warehouse

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[1] / "include" / "sql"


@dag(
    schedule=None,
    catchup=False,
    description=(
        "Bootstraps the warehouse by running `include/sql/*.sql` in order: creates the raw "
        "schema and its tables. Idempotent (everything is IF NOT EXISTS). Trigger manually "
        "after `astro dev kill` wipes the warehouse, or after adding a migration file."
    ),
    tags=["warehouse", "bootstrap"],
)
def warehouse_bootstrap():

    @task
    def create_tables() -> None:
        sql_files = sorted(SQL_DIR.glob("*.sql"))
        if not sql_files:
            raise RuntimeError("No SQL files found in include/sql; nothing to bootstrap")

        conn = warehouse.connect()
        with conn, conn.cursor() as cur:
            for sql_file in sql_files:
                logger.info(f"Executing {sql_file.name}")
                cur.execute(sql_file.read_text())

    create_tables()


warehouse_bootstrap()

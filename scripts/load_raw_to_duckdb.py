"""
Loads the raw CSV extracts into a DuckDB warehouse file under a `raw` schema.

This stands in for a real warehouse load step (e.g. COPY INTO Snowflake,
or a Spark job writing to S3 + external tables). DuckDB is used so the
whole pipeline runs anywhere with zero external infra -- useful for a
portfolio project a reviewer can clone and run in under a minute.
"""

import os
import duckdb

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")

TABLES = ["customers", "products", "orders"]


def run():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    for table in TABLES:
        csv_path = os.path.join(RAW_DIR, f"{table}.csv")
        con.execute(
            f"""
            CREATE OR REPLACE TABLE raw.{table} AS
            SELECT * FROM read_csv_auto('{csv_path}', header=True)
            """
        )
        count = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        print(f"Loaded raw.{table}: {count} rows")
    con.close()


if __name__ == "__main__":
    run()

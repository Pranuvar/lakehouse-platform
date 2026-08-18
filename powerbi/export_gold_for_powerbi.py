"""
Exports every gold mart to Parquet under powerbi/data_export/, plus a
synthetic RLS mapping table -- the connection path a real interview
demo actually uses.

Why export instead of a live connector: DuckDB has no first-class
Power BI connector (a community ODBC driver exists but adds a system
dependency this repo can't verify from a Linux CI-style environment);
Parquet import is zero-driver, works identically on Windows/Mac, and
Power BI Desktop reads it natively (Get Data > Parquet). The DAX/TMDL
model in powerbi/FjordMartAnalytics.SemanticModel/ is written against
these exact table/column names -- point Power BI at this folder and the
model just works. If/when this project's warehouse target moves to
Snowflake (see dbt/profiles.yml), Power BI's native Snowflake connector
replaces this export entirely; nothing in the model itself changes.

Run: `docker compose --profile orchestration exec airflow-scheduler
python /opt/airflow/powerbi/export_gold_for_powerbi.py`
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

DUCKDB_PATH = os.environ.get("DBT_DUCKDB_PATH", "/opt/airflow/data/warehouse/lakehouse.duckdb")
OUTPUT_DIR = Path(__file__).parent / "data_export"

GOLD_TABLES = [
    "dim_customers", "dim_stores", "dim_products", "dim_date",
    "fct_orders", "fct_order_items", "fct_campaign_performance", "fct_clickstream_sessions",
]

# Synthetic RLS mapping -- the standard "who can see what" table dynamic
# RLS is built on. In a real deployment this would sync from the IdP/HR
# system, not be hand-written; five rows is enough to demo the pattern
# (see powerbi/README.md for how the DAX role actually uses it).
USER_COUNTRY_ACCESS = [
    ("regional.manager.ie@fjordmart.example", "IE"),
    ("regional.manager.gb@fjordmart.example", "GB"),
    ("regional.manager.de@fjordmart.example", "DE"),
    ("global.exec@fjordmart.example", "*"),  # '*' = all countries, handled in the DAX role filter
]


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    for table in GOLD_TABLES:
        out_path = OUTPUT_DIR / f"{table}.parquet"
        con.execute(f"COPY (SELECT * FROM main_marts.{table}) TO '{out_path}' (FORMAT PARQUET)")
        n = con.execute(f"SELECT count(*) FROM main_marts.{table}").fetchone()[0]
        print(f"  {table:30s} {n:>10,} rows -> {out_path.name}")

    con.close()

    import pandas as pd
    access_df = pd.DataFrame(USER_COUNTRY_ACCESS, columns=["user_email", "country_access"])
    access_path = OUTPUT_DIR / "dim_user_country_access.parquet"
    access_df.to_parquet(access_path, index=False)
    print(f"  {'dim_user_country_access':30s} {len(access_df):>10,} rows -> {access_path.name}")

    print(f"\nExported to {OUTPUT_DIR}/ -- open Power BI Desktop, Get Data > Parquet, "
          f"point at each file, or open powerbi/FjordMartAnalytics.pbip directly and "
          f"repoint the data source to this folder (see powerbi/README.md).")


if __name__ == "__main__":
    main()

"""
COPY ACTIVITY: Postgres OLTP -> bronze.

Pipeline metadata (the declarative shape an ADF pipeline JSON would
carry -- linked service, dataset, watermark column -- kept as data next
to the activity that implements it rather than in a separate generic
YAML+interpreter layer, which would be pure overhead for 4 fixed
pipelines):

    linked_service : oltp Postgres (see ingestion/config.py)
    activity       : Copy (full snapshot for dims + order_items/payments;
                     watermark-incremental for orders)
    sink           : s3://lakehouse/bronze/{customers,stores,products,
                     order_items,payments,orders}

Two different bronze-append strategies, both genuinely append-only:

  * Dimension + child tables (customers, stores, products, order_items,
    payments) have no reliable per-row `updated_at` to key off cheaply
    at this schema, so each run re-extracts the FULL table -- but lands
    it as a new `snapshot_date`-partitioned batch, not an overwrite.
    Bronze history is never destroyed; silver can diff consecutive
    snapshots for SCD-style change detection if it needs to. (Documented
    simplification: a production version would add `updated_at` to
    these tables too and go incremental -- noted in the cost/performance
    write-up as a "what I'd change" item.)

  * `orders` is a true incremental watermark pipeline, keyed on
    `updated_at` (not `order_ts` -- see seeders/seed_postgres_oltp.py for
    why that distinction is exactly what makes the late-arriving-fact
    scenario work). Partitioned by `order_month` (derived from
    `order_ts`), which keeps partition cardinality sane (~25 partitions
    across the 2-year history) and lines up with how the backfill
    scenario operates on whole months.
"""
from __future__ import annotations

import time

import pandas as pd
import psycopg2

from ingestion.config import pg_dsn
from ingestion.control_table import get_watermark, set_watermark
from ingestion.delta_writer import write_bronze

PIPELINE_NAME = "postgres_oltp"
ORDERS_WATERMARK_KEY = "postgres_oltp.orders"

FULL_SNAPSHOT_TABLES = ["customers", "stores", "products", "order_items", "payments"]
DEFAULT_WATERMARK = "1970-01-01T00:00:00+00:00"


def _extract_chunks(sql: str, params: tuple = (), chunk_size: int = 200_000):
    """Server-side cursor so a 5.6M-row table (order_items) never has to
    sit fully in the extraction process's memory at once."""
    conn = psycopg2.connect(pg_dsn())
    try:
        cur = conn.cursor(name=f"extract_{int(time.time() * 1000)}")
        cur.itersize = chunk_size
        cur.execute(sql, params)
        colnames = None
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            if colnames is None:
                colnames = [d[0] for d in cur.description]
            yield pd.DataFrame(rows, columns=colnames)
        cur.close()
    finally:
        conn.close()


def _copy_full_snapshot(table: str) -> int:
    snapshot_date = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    total = 0
    for i, chunk in enumerate(_extract_chunks(f"SELECT * FROM oltp.{table}")):
        chunk["snapshot_date"] = snapshot_date
        total += write_bronze(
            chunk, table, source_pipeline=PIPELINE_NAME, mode="append",
            schema_mode="merge", partition_by=["snapshot_date"],
        )
    return total


def _copy_orders_incremental() -> tuple[int, str]:
    watermark = get_watermark(ORDERS_WATERMARK_KEY, default=DEFAULT_WATERMARK)
    sql = "SELECT * FROM oltp.orders WHERE updated_at > %s ORDER BY updated_at"

    total = 0
    max_updated_at = watermark
    for chunk in _extract_chunks(sql, (watermark,)):
        chunk["order_month"] = pd.to_datetime(chunk["order_ts"]).dt.strftime("%Y-%m")
        total += write_bronze(
            chunk, "orders", source_pipeline=PIPELINE_NAME, mode="append",
            schema_mode="merge", partition_by=["order_month"],
        )
        batch_max = chunk["updated_at"].max()
        if str(batch_max) > max_updated_at:
            max_updated_at = str(batch_max)

    return total, max_updated_at


def run() -> dict:
    result = {"pipeline": PIPELINE_NAME, "tables": {}}

    for table in FULL_SNAPSHOT_TABLES:
        rows = _copy_full_snapshot(table)
        result["tables"][table] = rows
        print(f"[{PIPELINE_NAME}] {table}: {rows:,} rows (full snapshot)")

    orders_rows, new_watermark = _copy_orders_incremental()
    result["tables"]["orders"] = orders_rows
    print(f"[{PIPELINE_NAME}] orders: {orders_rows:,} rows (incremental, watermark -> {new_watermark})")

    set_watermark(ORDERS_WATERMARK_KEY, new_watermark, status="success", rows=orders_rows)
    result["orders_watermark"] = new_watermark
    return result


if __name__ == "__main__":
    import json

    print(json.dumps(run(), indent=2, default=str))

"""
BRONZE -> SILVER: order_items, payments -- re-ingestion dedupe only.

Same duplication mechanism as stores.py (full-snapshot bronze doubled
by a same-day re-run -- see customers.py's docstring for the root
cause), same fix: window by natural key, keep the latest `_ingested_at`.

Not MERGE-based like orders.py, because these ARE immutable once
created in this dataset (an order_item's quantity/price doesn't change
after the fact; a payment doesn't either -- refunds are modelled as a
new payment row with payment_status='refunded' in the seed data, not a
mutation of an existing one). A plain dedupe is the honest match for
data that doesn't actually change, and using MERGE here would imply a
mutability story that isn't real -- see orders.py for where MERGE
actually earns its complexity.

Also enforces the one real cross-table invariant these two tables have
to satisfy together, right here at the boundary rather than leaving it
implicit: every order_item and payment must reference an order_id that
exists in bronze.orders. Rows that don't (there shouldn't be any --
this is a referential-integrity assertion, not a filter expected to
drop anything) are logged loudly rather than silently dropped, because
a silent `WHERE order_id IN (...)` here would be exactly the kind of
"tests report after the fact" pattern the brief explicitly wants this
project to avoid.
"""
from __future__ import annotations

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_session import get_spark, lakehouse_path


def _dedupe_latest(df, natural_key: str):
    w = Window.partitionBy(natural_key).orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "snapshot_date", "_ingested_at", "_source_pipeline")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )


def run() -> None:
    spark = get_spark("bronze_to_silver_order_items_payments")
    spark.sparkContext.setLogLevel("WARN")

    valid_order_ids = spark.read.format("delta").load(lakehouse_path("silver", "orders")).select("order_id")

    for table, natural_key in [("order_items", "order_item_id"), ("payments", "payment_id")]:
        bronze = spark.read.format("delta").load(lakehouse_path("bronze", table))
        raw_count = bronze.count()

        silver = _dedupe_latest(bronze, natural_key)
        deduped_count = silver.count()

        orphans = silver.join(valid_order_ids, "order_id", "left_anti").count()
        if orphans > 0:
            print(f"WARNING: {orphans:,} {table} rows reference an order_id not present in silver.orders", file=sys.stderr)

        (
            silver.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(lakehouse_path("silver", table))
        )
        print(f"{table}: bronze {raw_count:,} -> silver {deduped_count:,}  (orphaned order_id refs: {orphans:,})")

    spark.stop()


if __name__ == "__main__":
    run()

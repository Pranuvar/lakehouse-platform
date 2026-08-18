"""
BRONZE -> SILVER: orders -- the late-arriving-fact / backfill workhorse.

This is a plain `MERGE INTO silver.orders USING bronze.orders ON
order_id`, re-run over the FULL bronze table every time rather than
tracking its own watermark. That's deliberate, not lazy: bronze.orders
is already watermark-incremental (ingestion/pipelines/postgres_oltp.py
only ever lands rows newer than its own watermark), so re-merging the
whole bronze table costs one scan of however many rows have accumulated
there -- and in exchange, this job is trivially idempotent and safe to
re-run for ANY reason (a retry, a backfill, a late-arriving row showing
up in bronze days after its business date) without needing its own
separate watermark to reason about. Two moving cursors (one in
ingestion, a second one here) would be two places to get out of sync;
one is enough.

This is also THE mechanism that makes late-arriving facts and backfills
correct, not just theoretically correct:

  * Late-arriving fact: a row's presence in bronze is driven by
    `updated_at`/`created_at` (system time), but `MERGE ... ON
    t.order_id = s.order_id` keys purely on the business identity. When
    a late order finally lands in bronze days after its `order_ts`, this
    MERGE inserts it into silver with its correct historical `order_ts`
    and `order_month` -- exactly as if it had never been late. See
    docs/incident-log.md #2 for the live demo (a new order inserted into
    an already-"closed" historical month, re-merged, verified correct).
  * Backfill, no double-counting: re-running this MERGE for a month
    that's already fully represented in silver produces `whenMatched`
    UPDATEs (no-ops, since the values haven't changed) for every row
    already present -- never a second INSERT of the same order_id. See
    docs/incident-log.md #3 for the live demo (same month, merged twice,
    identical row counts and aggregates both times).

`whenMatchedUpdate` intentionally overwrites every tracked column, not
just a changed subset -- orders in this dataset can have their
`order_status`/`updated_at` legitimately change after creation (a
refund processed days later, see seeders/seed_postgres_oltp.py), and a
partial-update MERGE would silently miss that.

One more thing this job has to defend against, found the hard way: the
bronze source is deduplicated by `order_id` (latest `_ingested_at`
wins) before the MERGE even though bronze.orders is "incremental" and
you'd expect one row per order_id already. It isn't guaranteed under
every operational scenario -- specifically, manually resetting a
pipeline's watermark backward during an incident (a legitimate recovery
action, not a bug in itself) can cause already-ingested orders to be
re-swept into bronze a second time, giving bronze two rows for the same
order_id. Delta's MERGE correctly refuses to proceed when that happens
(`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` -- ambiguous
which source row should win), rather than silently picking one. Hit
this directly during the late-arriving-fact incident demo (see
docs/BUILD_LOG.md); the fix is this dedupe step, which makes the job
robust to bronze containing duplicates for whatever reason, not just
the one that happened to trigger it.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_session import get_spark, lakehouse_path

SILVER_COLUMNS = [
    "order_id", "customer_id", "store_id", "channel", "order_status",
    "currency", "order_ts", "created_at", "updated_at", "order_month",
]


def run() -> None:
    spark = get_spark("bronze_to_silver_orders")
    spark.sparkContext.setLogLevel("WARN")

    bronze_raw = spark.read.format("delta").load(lakehouse_path("bronze", "orders")).select(
        *SILVER_COLUMNS, "_ingested_at"
    )
    raw_count = bronze_raw.count()

    dedupe_w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    bronze = (
        bronze_raw.withColumn("_rn", F.row_number().over(dedupe_w))
        .filter(F.col("_rn") == 1)
        .select(*SILVER_COLUMNS)
    )
    bronze_count = bronze.count()
    if bronze_count < raw_count:
        print(f"note: bronze had {raw_count - bronze_count:,} duplicate order_id row(s) "
              f"(e.g. from a watermark reset re-sweeping already-ingested orders) -- deduped before merging")

    silver_path = lakehouse_path("silver", "orders")

    if not DeltaTable.isDeltaTable(spark, silver_path):
        bronze.write.format("delta").partitionBy("order_month").mode("overwrite").save(silver_path)
        print(f"initial load: {bronze_count:,} orders")
        spark.stop()
        return

    target = DeltaTable.forPath(spark, silver_path)
    before_count = target.toDF().count()

    (
        target.alias("t")
        .merge(bronze.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    after_count = spark.read.format("delta").load(silver_path).count()
    print(f"bronze orders scanned: {bronze_count:,}")
    print(f"silver orders before : {before_count:,}")
    print(f"silver orders after  : {after_count:,}")
    print(f"net new orders       : {after_count - before_count:,}")

    spark.stop()


if __name__ == "__main__":
    run()

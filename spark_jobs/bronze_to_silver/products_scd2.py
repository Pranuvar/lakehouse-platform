"""
BRONZE -> SILVER: products -- true SCD Type 2, via a native Delta `MERGE`,
not a dbt snapshot macro.

Worth being explicit about why this file exists at all: [retail-medallion-
pipeline](https://github.com/Pranuvar/retail-medallion-pipeline), the
first portfolio project, already implements SCD2 -- in dbt, via
`snapshot` blocks and the `dbt_utils` change-tracking strategy. Repeating
that here would prove nothing new. This implements the *same concept*
with a different engine, on purpose, so there's a real answer to "you've
done SCD2 before -- how would you do it in Spark instead of dbt, and
what's actually different?": dbt snapshots compare a full table scan
against history on every run; this uses `DeltaTable.merge()` with a
synthetic `merge_key` trick to do the update-old-row / insert-new-row
pair in a SINGLE atomic MERGE statement, which is the standard
Databricks-documented pattern for SCD2 on Delta and avoids the classic
two-step (update then insert) race window entirely.

Why products, not customers or stores: it needs an attribute that
realistically changes post-creation. `unit_price_eur` / `unit_cost_eur`
/ `is_active` genuinely do (retail pricing changes constantly) --
customers' and stores' attributes in this dataset don't, which is why
those two get simpler dedup jobs instead (see their own docstrings).

The MERGE pattern, concretely:
  1. Diff the latest bronze snapshot against silver's CURRENT rows
     (is_current = true) on (unit_cost_eur, unit_price_eur, is_active).
  2. Build one staged DataFrame containing, per changed product, TWO
     rows: one tagged `merge_key = product_id` (matches the existing
     current row -> closes it out) and one tagged `merge_key = NULL`
     (matches nothing -> forces an insert of the new current version).
     Brand-new products only get the insert-tagged row.
  3. One `.merge(...).whenMatchedUpdate(...).whenNotMatchedInsert(...)`
     call does both the close-out and the new-version-insert atomically.

Convention: `valid_to` is an EXCLUSIVE upper bound (a row is current for
`valid_from <= as_of < valid_to`), so consecutive versions never overlap
and a NULL `valid_to` unambiguously means "still current."
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from delta.tables import DeltaTable

from common.spark_session import get_spark, lakehouse_path

TRACKED_COLUMNS = ["unit_cost_eur", "unit_price_eur", "is_active"]
SILVER_COLUMNS = [
    "product_id", "sku", "product_name", "category", "subcategory", "brand",
    "unit_cost_eur", "unit_price_eur", "is_active",
]


def _latest_bronze_snapshot(spark):
    bronze = spark.read.format("delta").load(lakehouse_path("bronze", "products"))
    latest_snapshot_date = bronze.agg(F.max("snapshot_date")).collect()[0][0]

    per_id = Window.partitionBy("product_id").orderBy(F.col("_ingested_at").desc())
    incoming = (
        bronze.filter(F.col("snapshot_date") == latest_snapshot_date)
        .withColumn("_rn", F.row_number().over(per_id))
        .filter(F.col("_rn") == 1)
        # created_at is kept alongside the SILVER_COLUMNS set (not part
        # of it -- see the initial-load note in run() for why) so the
        # very first SCD2 load can backdate valid_from correctly.
        .select(*SILVER_COLUMNS, "created_at")
    )
    return incoming, latest_snapshot_date


def run() -> None:
    spark = get_spark("bronze_to_silver_products_scd2")
    spark.sparkContext.setLogLevel("WARN")

    silver_path = lakehouse_path("silver", "products")
    incoming, as_of_date = _latest_bronze_snapshot(spark)
    print(f"processing bronze snapshot as of {as_of_date}, {incoming.count():,} products")

    table_exists = DeltaTable.isDeltaTable(spark, silver_path)

    if not table_exists:
        # First run: every product is "new" -- no diff to compute yet.
        # valid_from is backdated to each product's own `created_at`
        # (its real catalog-entry date), NOT `as_of_date` (today, the
        # date this pipeline happens to be running for the first time).
        # Found the hard way: an earlier version used as_of_date here,
        # which meant every product's initial version was only "valid"
        # from today onward -- fine for a pipeline that's been running
        # since the business opened, but this project's entire 2-year
        # order history was seeded and processed within one demo
        # session, so EVERY historical order failed the point-in-time
        # join in fct_order_items.sql (5,613,938 of 5,625,033 rows --
        # 99.8% -- came back with a NULL margin on the first `dbt
        # build`). A product's price has been "whatever it was set to
        # at creation" since it was created, not since the SCD2 table
        # happened to be built -- backdating valid_from to created_at
        # is the historically honest fix, not a workaround.
        initial = (
            incoming.withColumn("valid_from", F.to_date(F.col("created_at")))
            .withColumn("valid_to", F.lit(None).cast("string"))
            .withColumn("is_current", F.lit(True))
            .withColumn("_silver_processed_at", F.current_timestamp())
            .select(*SILVER_COLUMNS, "valid_from", "valid_to", "is_current", "_silver_processed_at")
        )
        initial.write.format("delta").mode("overwrite").save(silver_path)
        print(f"initial load: {initial.count():,} products, valid_from backdated to each product's created_at")
        spark.stop()
        return

    target = DeltaTable.forPath(spark, silver_path)
    current = target.toDF().filter(F.col("is_current") == True)  # noqa: E712

    diff_condition = F.lit(False)
    for col in TRACKED_COLUMNS:
        diff_condition = diff_condition | (F.col(f"s.{col}") != F.col(f"c.{col}"))

    changed = (
        incoming.alias("s")
        .join(current.alias("c"), F.col("s.product_id") == F.col("c.product_id"))
        .where(diff_condition)
        .select("s.*")
    )
    new_products = incoming.join(current.select("product_id"), "product_id", "left_anti")

    # Materialise these counts NOW, into plain Python ints, before calling
    # merge(). `changed`/`new_products` are lazy DataFrames built from a
    # transformation of `current`, which is itself read from the silver
    # Delta table -- after `.execute()` mutates that same table, a
    # LATER `.count()` on these "same" DataFrames re-evaluates against
    # the now-POST-merge table state (Spark re-reads the source lazily
    # on every action), not the diff that was actually acted on. Caught
    # directly: an earlier version counted after `.execute()` and
    # reported "0 changed products" on a run that had just correctly
    # versioned 500 of them -- the actual data was always right, only
    # the diagnostic print was reading the wrong point in time.
    n_changed = changed.count()
    n_new = new_products.count()

    to_insert = (
        changed.unionByName(new_products)
        .withColumn("valid_from", F.lit(as_of_date))
        .withColumn("valid_to", F.lit(None).cast("string"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("merge_key", F.lit(None).cast(LongType()))
    )
    to_close = changed.withColumn("merge_key", F.col("product_id"))

    staged = to_insert.unionByName(to_close, allowMissingColumns=True)

    (
        target.alias("t")
        .merge(staged.alias("s"), "t.product_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            set={"is_current": "false", "valid_to": F.lit(as_of_date)}
        )
        .whenNotMatchedInsert(
            values={c: f"s.{c}" for c in SILVER_COLUMNS}
            | {
                "valid_from": "s.valid_from",
                "valid_to": "s.valid_to",
                "is_current": "s.is_current",
                "_silver_processed_at": "s._silver_processed_at",
            }
        )
        .execute()
    )

    result = spark.read.format("delta").load(silver_path)
    n_current = result.filter(F.col("is_current") == True).count()  # noqa: E712
    n_total = result.count()
    print(f"changed products (new price version): {n_changed:,}")
    print(f"brand-new products: {n_new:,}")
    print(f"silver.products: {n_current:,} current rows, {n_total:,} total rows (incl. history)")

    spark.stop()


if __name__ == "__main__":
    run()

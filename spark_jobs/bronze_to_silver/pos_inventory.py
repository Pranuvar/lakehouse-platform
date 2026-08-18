"""
BRONZE -> SILVER: pos_inventory_snapshots -- this is where the flat-file
schema drift actually gets resolved (see docs/incident-log.md #1 and
ingestion/pipelines/flat_files.py for the bronze half of this story).

Bronze deliberately did the minimum: force the volatile columns to
string so appends never hit a type-conflict write failure, and let
genuinely renamed columns (`unit_cost_eur` -> `unit_cost`) simply
coexist as two nullable columns rather than aliasing one to the other.
Silver does the rest, in one pass:

  1. `coalesce(unit_cost_eur, unit_cost)` -- collapses the rename. A row
     from before the rename has `unit_cost_eur` populated and
     `unit_cost` null; a row from after has the reverse. Exactly one of
     the two is ever non-null for a given row, so coalesce is lossless.
  2. Strip the currency prefix (`"EUR 5.36"` -> `5.36`) and the units
     suffix (`"42 units"` -> `42`) via regex, then cast to the real
     numeric types. Every value goes through this, not just the ones
     that look drifted -- bronze stores these columns as string
     UNCONDITIONALLY (see flat_files.py), so a clean-looking "42" and a
     drifted "42 units" are indistinguishable by dtype and have to be
     handled by the same regex either way.
  3. Drop exact duplicate rows on the natural key
     (store_id, product_sku, snapshot_date) -- the seeder's simulated
     "till retried its upload" duplicates, kept latest `_ingested_at`.

A row that fails to parse as numeric after stripping (shouldn't happen
given the seeder's generation rules, but "shouldn't happen" is exactly
the kind of assumption this project is about not trusting blindly) is
counted and reported rather than silently coerced to NULL and lost.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_session import get_spark, lakehouse_path


def _strip_to_number(col, pattern: str):
    """Extracts the first numeric token out of a string column, e.g.
    'EUR 5.36' -> '5.36', '42 units' -> '42'. Returns NULL (not an
    error) for anything that doesn't contain a numeric token at all --
    counted separately as unparseable, see run()."""
    return F.regexp_extract(col, pattern, 0)


def run() -> None:
    spark = get_spark("bronze_to_silver_pos_inventory")
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(lakehouse_path("bronze", "pos_inventory_snapshots"))
    raw_count = bronze.count()

    has_unit_cost = "unit_cost" in bronze.columns
    unit_cost_expr = (
        F.coalesce(F.col("unit_cost_eur"), F.col("unit_cost")) if has_unit_cost else F.col("unit_cost_eur")
    )

    cleaned = bronze.withColumn("_unit_cost_raw", unit_cost_expr).withColumn(
        "_qty_raw", F.col("quantity_on_hand")
    )

    NUMERIC_PATTERN = r"(\d+\.?\d*)"
    cleaned = (
        cleaned.withColumn("unit_cost_eur_clean", _strip_to_number(F.col("_unit_cost_raw"), NUMERIC_PATTERN).cast("decimal(10,2)"))
        .withColumn("quantity_on_hand_clean", _strip_to_number(F.col("_qty_raw"), NUMERIC_PATTERN).cast("int"))
    )

    unparseable_cost = cleaned.filter(F.col("_unit_cost_raw").isNotNull() & F.col("unit_cost_eur_clean").isNull()).count()
    unparseable_qty = cleaned.filter(F.col("_qty_raw").isNotNull() & F.col("quantity_on_hand_clean").isNull()).count()

    natural_key = ["store_id", "product_sku", "snapshot_date"]
    w = Window.partitionBy(*natural_key).orderBy(F.col("_ingested_at").desc())

    silver = (
        cleaned.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "store_id", "product_sku", "snapshot_date",
            F.col("quantity_on_hand_clean").alias("quantity_on_hand"),
            F.col("unit_cost_eur_clean").alias("unit_cost_eur"),
            F.col("reorder_point"),  # nullable pre-stage-1, real int elsewhere -- no drift on this one
            "drop_year_month",
            F.current_timestamp().alias("_silver_processed_at"),
        )
    )

    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("drop_year_month")
        .save(lakehouse_path("silver", "pos_inventory_snapshots"))
    )

    final_count = silver.count()
    print(f"bronze rows                : {raw_count:,}")
    print(f"silver rows (deduped)      : {final_count:,}")
    print(f"duplicate rows dropped     : {raw_count - final_count:,}")
    print(f"unparseable unit_cost_eur  : {unparseable_cost:,}")
    print(f"unparseable quantity_on_hand: {unparseable_qty:,}")

    spark.stop()


if __name__ == "__main__":
    run()

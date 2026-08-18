"""
BRONZE -> SILVER: customers -- identity resolution, not SCD2.

Two independent kinds of duplication land in bronze.customers, and they
need two different fixes, applied in order:

  1. Re-ingestion duplication: `postgres_oltp` is a full-snapshot copy
     activity (see ingestion/pipelines/postgres_oltp.py's docstring for
     why), landed append-only and partitioned by `snapshot_date`. Run
     the pipeline twice on the same calendar day (which happened during
     this build -- see docs/BUILD_LOG.md) and the same 153,000 rows
     land twice under the same partition. Fix: window by `customer_id`,
     keep the row with the latest `_ingested_at`. This is a mechanical
     "collapse re-ingests" step, nothing business-specific about it.

  2. Identity duplication: ~2% of `customer_id`s in the OLTP source are
     DELIBERATELY the same real person twice -- a guest checkout that
     later registered an account, seeded on purpose in
     seeders/seed_postgres_oltp.py with cosmetic email drift (extra
     whitespace / different casing). This is genuinely a different
     problem: not "the same row landed twice," but "two different
     natural keys refer to the same entity." Fixed via identity
     resolution on `lower(trim(email))`: the earliest `customer_id`
     sharing a normalised email wins as the canonical record, and every
     `customer_id` that collapsed into it is kept in
     `source_customer_ids` -- so a downstream question like "why did
     customer 153042 disappear from silver?" has a direct, traceable
     answer instead of a silently vanished row.

Why not SCD2 here: SCD2 tracks an entity's attributes changing over
time (see products.py for where that pattern actually gets used in this
project). Customers in this dataset don't have a meaningful "changed
address" story -- the real problem is duplicate identities, which is an
entity-resolution problem, not a slowly-changing-dimension problem.
Using the same pattern for both would be the wrong tool for one of them.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_session import get_spark, lakehouse_path


def run() -> None:
    spark = get_spark("bronze_to_silver_customers")
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(lakehouse_path("bronze", "customers"))
    raw_count = bronze.count()

    # Stage 1: collapse re-ingestion duplicates -- one row per customer_id.
    latest_per_id = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    deduped = (
        bronze.withColumn("_rn", F.row_number().over(latest_per_id))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("normalized_email", F.lower(F.trim(F.col("email"))))
    )
    stage1_count = deduped.count()

    # Stage 2: identity resolution -- one row per real person. Canonical
    # customer_id = the earliest one sharing a normalised email (the
    # original registration, not the later guest-checkout duplicate).
    canonical_id = Window.partitionBy("normalized_email").orderBy(F.col("customer_id").asc())
    with_canonical = deduped.withColumn("canonical_customer_id", F.first("customer_id").over(canonical_id))

    source_ids = (
        with_canonical.groupBy("canonical_customer_id")
        .agg(F.sort_array(F.collect_set("customer_id")).alias("source_customer_ids"))
    )

    resolved = (
        with_canonical.filter(F.col("customer_id") == F.col("canonical_customer_id"))
        .join(source_ids, "canonical_customer_id")
        .select(
            F.col("canonical_customer_id").alias("customer_id"),
            "first_name", "last_name",
            F.col("normalized_email").alias("email"),
            "country", "city", "signup_date", "is_marketing_opt_in",
            "source_customer_ids",
            F.size("source_customer_ids").alias("merged_identity_count"),
            F.current_timestamp().alias("_silver_processed_at"),
        )
    )

    (
        resolved.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(lakehouse_path("silver", "customers"))
    )

    final_count = resolved.count()
    merged = resolved.filter(F.col("merged_identity_count") > 1).count()
    print(f"bronze rows           : {raw_count:,}")
    print(f"after re-ingest dedupe: {stage1_count:,}")
    print(f"after identity resolve: {final_count:,}")
    print(f"customers with merged duplicate identities: {merged:,}")

    spark.stop()


if __name__ == "__main__":
    run()

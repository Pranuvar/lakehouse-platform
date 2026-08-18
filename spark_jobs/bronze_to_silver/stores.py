"""
BRONZE -> SILVER: stores -- plain re-ingestion dedupe, nothing more.

Unlike customers (identity resolution) or products (SCD2), stores has
neither problem in this dataset: no duplicate real-world entities, and
no attribute that realistically changes post-open (name/channel/country/
city/opened_date are all set-once). The only cleanup needed is
collapsing the same `snapshot_date`-doubling every full-snapshot bronze
table has -- see customers.py's docstring for why that doubling exists.
Kept as its own tiny job rather than folded into a generic "dedupe any
dimension" helper: three call sites isn't enough repetition to justify
an abstraction over three very short, very readable scripts.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_session import get_spark, lakehouse_path


def run() -> None:
    spark = get_spark("bronze_to_silver_stores")
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(lakehouse_path("bronze", "stores"))
    raw_count = bronze.count()

    latest_per_id = Window.partitionBy("store_id").orderBy(F.col("_ingested_at").desc())
    silver = (
        bronze.withColumn("_rn", F.row_number().over(latest_per_id))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "snapshot_date", "_ingested_at", "_source_pipeline")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(lakehouse_path("silver", "stores"))
    )

    final_count = silver.count()
    print(f"bronze rows: {raw_count:,}  ->  silver rows: {final_count:,}")
    spark.stop()


if __name__ == "__main__":
    run()

"""
BRONZE -> SILVER: clickstream_events -- defensive dedupe, mostly a
passthrough.

Unlike the other silver jobs, this one isn't cleaning up a duplication
mechanism that's known to exist in the seed data -- the Kafka consumer
(ingestion/pipelines/kafka_events.py) commits offsets only AFTER a
batch is durably written to bronze, specifically to avoid double-
delivery. But "commits after write" makes redelivery on a crash
*unlikely*, not impossible (a crash between the write succeeding and
the commit landing is still an at-least-once gap, by design -- see that
pipeline's own docstring). Deduping on `event_id` here is the cheap,
correct safety net for that gap: harmless if there's nothing to catch,
correct if there ever is.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_session import get_spark, lakehouse_path


def run() -> None:
    spark = get_spark("bronze_to_silver_clickstream_events")
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(lakehouse_path("bronze", "clickstream_events"))
    raw_count = bronze.count()

    w = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").desc())
    silver = (
        bronze.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_ingested_at", "_source_pipeline")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(lakehouse_path("silver", "clickstream_events"))
    )

    final_count = silver.count()
    print(f"bronze rows: {raw_count:,}  ->  silver rows: {final_count:,}  (redelivered duplicates removed: {raw_count - final_count:,})")

    spark.stop()


if __name__ == "__main__":
    run()

"""
BRONZE -> SILVER: campaign_performance -- resolves the mock ad API's
restatement behaviour (see docker/mock-api/app.py's docstring: a
time-rotating slice of historical rows re-appears with a fresh
`updated_at` on every poll, simulating an ad platform correcting
attribution after the fact).

Natural key is (campaign_id, ad_set_id, date) -- NOT campaign_id alone,
since each campaign has multiple ad sets and each ad set reports daily.
A given natural key can appear multiple times in bronze across separate
ingestion runs (original ingest + any number of restatements picked up
by later incremental syncs); the correct row is always the one with the
MAX `updated_at`, which is precisely what "restatement" means -- a
later correction should win over an earlier value, not the other way
round and not an arbitrary one.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark_session import get_spark, lakehouse_path


def run() -> None:
    spark = get_spark("bronze_to_silver_campaign_performance")
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(lakehouse_path("bronze", "campaign_performance"))
    raw_count = bronze.count()

    natural_key = ["campaign_id", "ad_set_id", "date"]
    w = Window.partitionBy(*natural_key).orderBy(F.col("updated_at").desc())

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
        .partitionBy("channel")
        .save(lakehouse_path("silver", "campaign_performance"))
    )

    final_count = silver.count()
    print(f"bronze rows (incl. restatement re-observations): {raw_count:,}")
    print(f"silver rows (one per campaign/ad_set/date, latest updated_at wins): {final_count:,}")
    print(f"restated rows resolved: {raw_count - final_count:,}")

    spark.stop()


if __name__ == "__main__":
    run()

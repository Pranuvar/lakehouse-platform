"""
Helper for incident_04_crash_recovery.py -- NOT a real pipeline job.
Overwrites s3://lakehouse/_ops/_crash_test with a large DataFrame
(all of bronze.order_items, ~11M rows, repartitioned wide to spread the
write across enough files/tasks to have a real window where SIGKILL
lands mid-write rather than before-or-after it). Run standalone to
measure timing, or killed mid-flight by the incident script.
"""
from __future__ import annotations

import os

from common.spark_session import get_spark, lakehouse_path

_BUCKET = os.environ.get("MINIO_BUCKET_LAKEHOUSE", "lakehouse")
CRASH_TEST_PATH = f"s3a://{_BUCKET}/_ops/_crash_test"


def run() -> None:
    spark = get_spark("crash_test_big_write", shuffle_partitions=64)
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(lakehouse_path("bronze", "order_items")).repartition(64)
    df.write.format("delta").mode("overwrite").save(CRASH_TEST_PATH)

    spark.stop()


if __name__ == "__main__":
    run()

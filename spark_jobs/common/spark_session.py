"""
Shared SparkSession builder for every bronze/silver job.

Two things worth explaining in an interview:

1. Delta Lake needs its own catalog/extensions wired in
   (`spark.sql.extensions` / `spark.sql.catalog.spark_catalog`) -- this is
   what turns `df.write.format("delta")` into something with a real
   transaction log (`_delta_log/`), not just parquet-with-a-label. Done
   via `delta_spark.configure_spark_with_delta_pip`-equivalent config
   here (set explicitly rather than through the helper, so the packages
   list is visible and auditable in one place).

2. S3A (the Hadoop filesystem client Spark uses to talk to MinIO/S3) is
   NOT bundled with PySpark's pip wheel -- `hadoop-aws` and
   `aws-java-sdk-bundle` are pulled from Maven at spark-submit time via
   `spark.jars.packages`, cached under ~/.ivy2 after the first run. The
   versions below (hadoop-aws 3.3.4) are pinned to match the Hadoop
   client version PySpark 3.5.3 itself ships with -- a mismatch here is
   the single most common cause of a Spark-to-S3-compatible-storage setup
   silently failing with an opaque `NoSuchMethodError`.

`local[*]` is the Spark master here on purpose (see docker-compose.yml's
Airflow-image note) -- swapping to a real cluster is a one-line change to
`master`, nothing else in a job changes.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "lakehouse")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse_dev_pw")

HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"
DELTA_VERSION = "3.2.1"  # matches delta-spark installed in docker/airflow/requirements.txt


def get_spark(app_name: str, shuffle_partitions: int = 8) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config(
            "spark.jars.packages",
            f"io.delta:delta-spark_2.12:{DELTA_VERSION},"
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION},"
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # -- S3A / MinIO wiring --
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # small local dataset -- default 200 shuffle partitions is pure overhead
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    )
    return builder.getOrCreate()


def lakehouse_path(layer: str, table: str, bucket: str | None = None) -> str:
    """s3a://<lakehouse bucket>/<bronze|silver|gold>/<table> -- the one
    place the bronze/silver/gold path convention is defined, so every job
    and every dbt source points at the same layout."""
    bucket = bucket or os.environ.get("MINIO_BUCKET_LAKEHOUSE", "lakehouse")
    assert layer in {"bronze", "silver", "gold"}, f"unknown layer: {layer}"
    return f"s3a://{bucket}/{layer}/{table}"

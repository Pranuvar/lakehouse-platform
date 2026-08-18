"""
Connection config for the ingestion layer. Unlike seeders/common.py (host
venv, published ports on localhost), this code runs INSIDE the
airflow-scheduler container on the compose network, so every default here
is the in-network service name -- see docker-compose.yml's
`x-airflow-common` environment block for where these env vars come from.
"""
from __future__ import annotations

import os

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "lakehouse")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "lakehouse_dev_pw")
POSTGRES_OLTP_DB = os.environ.get("POSTGRES_OLTP_DB", "oltp")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "lakehouse")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse_dev_pw")
MINIO_BUCKET_DROPZONE = os.environ.get("MINIO_BUCKET_DROPZONE", "raw-dropzone")
MINIO_BUCKET_LAKEHOUSE = os.environ.get("MINIO_BUCKET_LAKEHOUSE", "lakehouse")

REDPANDA_BROKER = os.environ.get("REDPANDA_BROKER", "redpanda:9092")
REDPANDA_TOPIC_EVENTS = os.environ.get("REDPANDA_TOPIC_EVENTS", "web.clickstream.events")

MOCK_API_BASE_URL = os.environ.get("MOCK_API_BASE_URL", "http://mock-api:8000")
MOCK_API_KEY = os.environ.get("MOCK_API_KEY", "dev-local-key-do-not-use-in-prod")


def pg_dsn() -> str:
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_OLTP_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )


def delta_storage_options() -> dict:
    """storage_options for delta-rs (deltalake package) talking to MinIO.
    AWS_S3_ALLOW_UNSAFE_RENAME is required against MinIO/any backend that
    doesn't support the atomic conditional-put S3 uses natively for
    concurrent-write safety -- fine here since these are single-writer
    batch pipelines, not concurrent streaming writers."""
    return {
        "AWS_ACCESS_KEY_ID": MINIO_ROOT_USER,
        "AWS_SECRET_ACCESS_KEY": MINIO_ROOT_PASSWORD,
        "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def bronze_path(table: str) -> str:
    # `s3://`, not `s3a://` -- delta-rs's Rust object_store client uses
    # the plain s3 scheme; PySpark's Hadoop S3A connector (spark_jobs/)
    # uses s3a://. Same MinIO bucket/objects either way, two different
    # client libraries with two different URI conventions -- see
    # docs/BUILD_LOG.md for why the ingestion layer deliberately uses
    # delta-rs instead of Spark here.
    return f"s3://{MINIO_BUCKET_LAKEHOUSE}/bronze/{table}"

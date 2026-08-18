"""
Watermark control table -- the ADF-pattern piece that makes incremental
copy activities actually incremental. Real ADF pipelines commonly track
"last successful watermark value" in a small control table in a database
they already have access to, read it at the start of a pipeline run,
extract everything newer than it, and advance it only after a
successful run. This is that table, living in the OLTP Postgres instance
(the one control DB every pipeline already has network access to).

One row per pipeline. `watermark_value` is stored as TEXT deliberately --
different pipelines key off different things (a timestamp for
Postgres/REST, a Kafka offset for the event stream), so this is a
generic cursor store, not a timestamp-typed column that would only fit
some of the four pipelines.
"""
from __future__ import annotations

from datetime import datetime, timezone

import psycopg2

from ingestion.config import pg_dsn

BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.pipeline_watermarks (
    pipeline_name    TEXT PRIMARY KEY,
    watermark_value  TEXT,
    last_run_status  TEXT,
    rows_last_run    BIGINT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_bootstrapped() -> None:
    with psycopg2.connect(pg_dsn()) as conn, conn.cursor() as cur:
        cur.execute(BOOTSTRAP_SQL)
        conn.commit()


def get_watermark(pipeline_name: str, default: str | None = None) -> str | None:
    ensure_bootstrapped()
    with psycopg2.connect(pg_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT watermark_value FROM ingestion.pipeline_watermarks WHERE pipeline_name = %s",
            (pipeline_name,),
        )
        row = cur.fetchone()
    return row[0] if row else default


def set_watermark(pipeline_name: str, value: str, status: str = "success", rows: int = 0) -> None:
    with psycopg2.connect(pg_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.pipeline_watermarks (pipeline_name, watermark_value, last_run_status, rows_last_run, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (pipeline_name) DO UPDATE SET
                watermark_value = EXCLUDED.watermark_value,
                last_run_status = EXCLUDED.last_run_status,
                rows_last_run   = EXCLUDED.rows_last_run,
                updated_at      = EXCLUDED.updated_at
            """,
            (pipeline_name, value, status, rows, datetime.now(timezone.utc)),
        )
        conn.commit()

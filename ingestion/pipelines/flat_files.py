"""
COPY ACTIVITY: MinIO flat-file drop zone -> bronze.

    linked_service : MinIO (raw-dropzone bucket)
    activity       : GetMetadata (list new month-folders) + Copy (per file)
    sink           : s3://lakehouse/bronze/pos_inventory_snapshots

This is the schema-drift absorption pipeline (interview scenario #1 --
see docs/incident-log.md). The design decision that makes it work:
bronze does NOT try to normalise the schema drift away. It does exactly
two things to every file before writing, and nothing more:

  1. Drops any stray `Unnamed: *` index column (a handful of source CSVs
     were exported with `index=True` by mistake -- this is junk, not
     schema, safe to discard at the boundary).
  2. Forces the columns known to drift in TYPE across the source's
     history (`quantity_on_hand`, `unit_cost_eur`, `unit_cost`) to
     string, unconditionally, on every file, whether that particular
     file's native type was already string or not.

That second point is the whole trick. A first attempt at this pipeline
left those columns in their native per-file type and let delta-rs's
`schema_mode="merge"` handle the rest -- which works fine for genuinely
NEW columns (`reorder_point` merges in cleanly, nullable in older rows)
but throws a hard cast error the moment two batches disagree on a
column's TYPE (int64 vs string) rather than just its presence. Proven
directly: see the type-conflict smoke test in docs/BUILD_LOG.md. Forcing
the volatile columns to string sidesteps that -- bronze ends up as text
for those fields regardless of source stage, which is honest (some rows
generally are text, "42 units"), and defers the real coercion (strip
units/currency, cast numeric) to silver, which is where "cleaned"
belongs in this architecture, not bronze.

Genuinely renamed columns (`unit_cost_eur` -> `unit_cost` in the last
two months) are NOT aliased here either -- both column names simply
exist in the bronze table, nullable depending on which era a row came
from. Silver does `coalesce(unit_cost_eur, unit_cost)`.

Duplicate rows within a file (the seeder's simulated retried-upload
mess) are landed as-is -- deduplication is explicitly a silver-layer
concern in this architecture, not bronze's.

Watermark: tracked at month-folder granularity (`YYYY-MM` of the last
fully-processed drop cycle), not per-file -- matches how these files
actually arrive (one batch per store per month) and avoids needing a
per-file tracking table for what is, in practice, a few thousand files
total.
"""
from __future__ import annotations

import io

import boto3
import pandas as pd
from botocore.client import Config

from ingestion.config import MINIO_BUCKET_DROPZONE, MINIO_ENDPOINT, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER
from ingestion.control_table import get_watermark, set_watermark
from ingestion.delta_writer import write_bronze

PIPELINE_NAME = "flat_files_pos_inventory"
DROPZONE_PREFIX = "pos-inventory-snapshots"
VOLATILE_STRING_COLUMNS = {"quantity_on_hand", "unit_cost_eur", "unit_cost"}
DEFAULT_WATERMARK = "0000-00"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def _list_month_folders(client) -> list[str]:
    """Returns sorted 'YYYY-MM' folders found under the dropzone prefix."""
    paginator = client.get_paginator("list_objects_v2")
    months = set()
    for page in paginator.paginate(Bucket=MINIO_BUCKET_DROPZONE, Prefix=f"{DROPZONE_PREFIX}/", Delimiter=""):
        for obj in page.get("Contents", []):
            # key shape: pos-inventory-snapshots/2024/08/store_0002_2024_08.csv
            parts = obj["Key"].split("/")
            if len(parts) >= 3:
                months.add(f"{parts[1]}-{parts[2]}")
    return sorted(months)


def _read_object(client, key: str) -> pd.DataFrame:
    body = client.get_object(Bucket=MINIO_BUCKET_DROPZONE, Key=key)["Body"].read()
    if key.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(body), engine="pyarrow")
    else:
        df = pd.read_csv(io.BytesIO(body))

    df = df.loc[:, ~df.columns.str.match(r"^Unnamed")]

    for col in VOLATILE_STRING_COLUMNS & set(df.columns):
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else str(v))

    df["_source_key"] = key
    return df


def run() -> dict:
    client = s3_client()
    watermark = get_watermark(PIPELINE_NAME, default=DEFAULT_WATERMARK)
    months = [m for m in _list_month_folders(client) if m > watermark]

    if not months:
        print(f"[{PIPELINE_NAME}] no new month-folders since watermark {watermark}")
        return {"pipeline": PIPELINE_NAME, "rows": 0, "months_processed": []}

    total_rows = 0
    for month in months:
        year, mon = month.split("-")
        prefix = f"{DROPZONE_PREFIX}/{year}/{mon}/"
        keys = [
            obj["Key"]
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=MINIO_BUCKET_DROPZONE, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]

        month_rows = 0
        for key in keys:
            df = _read_object(client, key)
            df["drop_year_month"] = month
            month_rows += write_bronze(
                df, "pos_inventory_snapshots", source_pipeline=PIPELINE_NAME,
                mode="append", schema_mode="merge", partition_by=["drop_year_month"],
            )
        print(f"[{PIPELINE_NAME}] {month}: {len(keys)} files, {month_rows:,} rows")
        total_rows += month_rows

    new_watermark = months[-1]
    set_watermark(PIPELINE_NAME, new_watermark, status="success", rows=total_rows)
    print(f"[{PIPELINE_NAME}] done: {total_rows:,} rows across {len(months)} months, watermark -> {new_watermark}")
    return {"pipeline": PIPELINE_NAME, "rows": total_rows, "months_processed": months}


if __name__ == "__main__":
    import json

    print(json.dumps(run(), indent=2, default=str))

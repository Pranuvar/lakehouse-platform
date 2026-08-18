"""
SOURCE 3: Flat-file drops -- monthly POS inventory-on-hand snapshots,
one file per physical store per month, dropped into MinIO's
`raw-dropzone` bucket exactly the way a till/back-office system would
batch-export and SFTP/upload a file: no schema contract, no coordination
with the analytics team, whatever the POS vendor's export format happened
to be that month.

This is deliberately the "messy schema" source, on purpose, in four
escalating stages across ~25 months of drops (see SCHEMA_STAGES below):

  stage 0  (months  0- 7): store_id, product_sku, snapshot_date,
                           quantity_on_hand, unit_cost_eur -- clean.
  stage 1  (months  8-16): + reorder_point                -- additive,
                           easy: bronze should just pick it up.
  stage 2  (months 17-22): unit_cost_eur starts arriving as "<EUR>12.34"
                           (a locale/formatting change upstream) and
                           quantity_on_hand is sometimes "123 units"
                           instead of a clean int -- a type-drift bronze
                           has to coerce, not just append.
  stage 3  (months 23-24): unit_cost_eur is RENAMED to unit_cost -- a
                           breaking rename bronze has to alias, or the
                           column silently goes missing downstream.

On top of the schema drift, individual files carry ordinary
real-world mess: a stray pandas-style unnamed index column on some CSV
drops, occasional duplicate rows (retried exports), and a slice of
blank quantity_on_hand values. File format alternates CSV/Parquet by
month so ingestion has to handle both, not just one.

Run: `python seeders/seed_flatfiles.py` (needs `docker compose up -d
minio minio-init`).
"""
from __future__ import annotations

import io
import time
from datetime import date, timedelta

import boto3
import numpy as np
import pandas as pd
from botocore.client import Config

from common import (
    HISTORY_DAYS,
    HISTORY_START,
    MINIO_BUCKET_DROPZONE,
    MINIO_ENDPOINT,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
    SEED,
    TODAY,
)

N_PHYSICAL_STORES = 149  # store_id 2..150, see seed_postgres_oltp.generate_stores
N_PRODUCTS_TOTAL = 20_000
ACTIVE_SKUS_PER_STORE = 500  # a store only ever reports its working assortment, not the full catalog

rng = np.random.default_rng(SEED + 1)

DROPZONE_PREFIX = "pos-inventory-snapshots"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def month_starts(start: date, end: date) -> list[date]:
    months = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        months.append(cursor)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


def schema_stage(month_index: int, total_months: int) -> int:
    """Maps a month's position in the drop history to one of 4 escalating
    schema-drift stages -- see module docstring."""
    if month_index < 8:
        return 0
    if month_index < 17:
        return 1
    if month_index < total_months - 2:
        return 2
    return 3


def active_skus_for_store(store_id: int) -> np.ndarray:
    """Deterministic per-store working assortment: same store reports
    (roughly) the same SKUs month over month, the way a real store's
    planogram doesn't reshuffle wholesale every 30 days."""
    store_rng = np.random.default_rng(SEED + store_id)
    return store_rng.choice(np.arange(1, N_PRODUCTS_TOTAL + 1), size=ACTIVE_SKUS_PER_STORE, replace=False)


def build_snapshot(store_id: int, snapshot_date: date, stage: int) -> pd.DataFrame:
    skus = active_skus_for_store(store_id)
    n = len(skus)
    file_rng = np.random.default_rng(SEED + store_id * 10_000 + snapshot_date.toordinal())

    quantity = file_rng.poisson(lam=40, size=n)
    unit_cost = np.round(file_rng.gamma(shape=2.0, scale=2.5, size=n) + 0.5, 2)

    df = pd.DataFrame(
        {
            "store_id": store_id,
            "product_sku": [f"SKU-{s:06d}" for s in skus],
            "snapshot_date": snapshot_date.isoformat(),
            "quantity_on_hand": quantity,
            "unit_cost_eur": unit_cost,
        }
    )

    if stage >= 1:
        df["reorder_point"] = file_rng.integers(10, 60, size=n)

    if stage >= 2:
        # ~30% of rows on the new till firmware that appends units / a
        # currency symbol as text instead of clean numerics. The WHOLE
        # column is re-typed to string (not just the drifted rows) --
        # that's how this actually shows up in a real export: once a
        # column stops being purely numeric, both CSV and (especially)
        # Parquet force every value in it to a single text type, so even
        # the still-clean-looking values ("42") arrive as text now.
        drift_mask = file_rng.random(n) < 0.30
        blank_mask = file_rng.random(n) < 0.02  # ~2% missing outright (export truncation)
        qty_text = np.where(drift_mask, df["quantity_on_hand"].astype(str) + " units", df["quantity_on_hand"].astype(str))
        df["quantity_on_hand"] = pd.Series(qty_text, dtype="object").mask(blank_mask, None)
        df["unit_cost_eur"] = df["unit_cost_eur"].apply(lambda v: f"EUR {v:.2f}")

    if stage == 3:
        # breaking rename: downstream must alias unit_cost -> unit_cost_eur
        df = df.rename(columns={"unit_cost_eur": "unit_cost"})

    # Ordinary file mess, independent of the schema stage:
    if file_rng.random() < 0.10:
        # a handful of duplicate rows -- the till retried an upload
        dupe_rows = df.sample(n=min(8, len(df)), random_state=int(file_rng.integers(0, 1_000_000)))
        df = pd.concat([df, dupe_rows], ignore_index=True)

    return df


def upload(client, key: str, df: pd.DataFrame, fmt: str, stray_index_column: bool) -> int:
    buf = io.BytesIO()
    if fmt == "csv":
        text_buf = io.StringIO()
        df.to_csv(text_buf, index=stray_index_column)
        buf.write(text_buf.getvalue().encode("utf-8"))
        content_type = "text/csv"
    else:
        df.to_parquet(buf, index=False, engine="pyarrow")
        content_type = "application/octet-stream"
    buf.seek(0)
    client.put_object(Bucket=MINIO_BUCKET_DROPZONE, Key=key, Body=buf.getvalue(), ContentType=content_type)
    return len(df)


def main() -> None:
    t0 = time.time()
    client = s3_client()

    months = month_starts(HISTORY_START, TODAY)
    total_months = len(months)
    print(f"{total_months} monthly drop cycles x {N_PHYSICAL_STORES} stores "
          f"x ~{ACTIVE_SKUS_PER_STORE} SKUs")

    total_rows = 0
    total_files = 0
    stage_counts = {0: 0, 1: 0, 2: 0, 3: 0}

    for month_idx, month_start in enumerate(months):
        stage = schema_stage(month_idx, total_months)
        stage_counts[stage] += 1
        fmt = "csv" if month_idx % 2 == 0 else "parquet"
        # snapshot taken on the last day of the month covered (or today, for the current month)
        if month_idx == total_months - 1:
            snapshot_date = TODAY
        else:
            next_month = months[month_idx + 1]
            snapshot_date = next_month - timedelta(days=1)

        for store_id in range(2, 2 + N_PHYSICAL_STORES):
            df = build_snapshot(store_id, snapshot_date, stage)
            stray_index = fmt == "csv" and rng.random() < 0.05
            key = (
                f"{DROPZONE_PREFIX}/{month_start:%Y}/{month_start:%m}/"
                f"store_{store_id:04d}_{month_start:%Y_%m}.{fmt}"
            )
            total_rows += upload(client, key, df, fmt, stray_index)
            total_files += 1

        print(
            f"  {month_start:%Y-%m}  stage={stage}  fmt={fmt:7s}  "
            f"files_so_far={total_files:,}  rows_so_far={total_rows:,}"
        )

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:,.1f}s.")
    print(f"  files : {total_files:,}")
    print(f"  rows  : {total_rows:,}")
    print(f"  schema stage distribution (months): {stage_counts}")
    print(
        "  stage 0 = base | stage 1 = +reorder_point | "
        "stage 2 = type drift (unit_cost_eur as text, messy quantity) | "
        "stage 3 = unit_cost_eur renamed -> unit_cost"
    )


if __name__ == "__main__":
    main()

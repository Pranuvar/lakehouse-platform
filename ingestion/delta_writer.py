"""
Shared bronze-write helper for every copy activity -- one place that
defines what "landing in bronze" means: append-only, schema merge
allowed (additive columns and, per the flat-file pipeline, occasionally
a genuinely new column name), every row stamped with ingestion metadata,
never a destructive overwrite of prior history.

Uses delta-rs (the `deltalake` package), not PySpark -- see
ingestion/config.py's `bronze_path()` docstring for why.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from ingestion.config import bronze_path, delta_storage_options


def write_bronze(
    df: pd.DataFrame,
    table: str,
    source_pipeline: str,
    mode: str = "append",
    schema_mode: str | None = "merge",
    partition_by: list[str] | None = None,
) -> int:
    """Writes df to s3://lakehouse/bronze/<table>, stamping ingestion
    metadata columns. Returns row count written."""
    if df.empty:
        return 0

    df = df.copy()
    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_source_pipeline"] = source_pipeline

    write_deltalake(
        bronze_path(table),
        df,
        mode=mode,
        schema_mode=schema_mode if mode == "append" else None,
        partition_by=partition_by,
        storage_options=delta_storage_options(),
    )
    return len(df)


def table_exists(table: str) -> bool:
    try:
        DeltaTable(bronze_path(table), storage_options=delta_storage_options())
        return True
    except TableNotFoundError:
        return False


def row_count(table: str) -> int:
    if not table_exists(table):
        return 0
    dt = DeltaTable(bronze_path(table), storage_options=delta_storage_options())
    return dt.to_pyarrow_dataset().count_rows()

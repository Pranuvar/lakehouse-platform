"""
INCIDENT #8 (partial -- file-compaction half): partitioning, small-file
compaction, and what actually changes because of it. See
docs/BUILD_LOG.md for the "before" evidence this was already sitting on:
`ingestion/pipelines/flat_files.py` writes one delta-rs commit PER
SOURCE FILE (3,725 of them across the seeded history) rather than
batching a month's files into one write -- a deliberate choice at the
time (see that pipeline's docstring), left in place specifically so it
could be real "before" evidence here instead of a synthetic example.

This script measures the actual cost of that: file count, average file
size, and a representative query's wall-clock time, before and after
running Delta's `OPTIMIZE` (file compaction) on the same table.

Run: `docker compose --profile orchestration exec airflow-scheduler
python /opt/airflow/ops/incident_08_compaction.py`
"""
from __future__ import annotations

import time

from common.spark_session import get_spark, lakehouse_path


def file_stats(spark, path: str) -> dict:
    # `DESCRIBE DETAIL` (Delta's own catalog of its data files), not a
    # manual Hadoop FS directory listing -- this table is partitioned
    # (drop_year_month), so an earlier version using `listStatus` on the
    # table root looked directly under the root and silently counted 0
    # files: partitioned tables keep their parquet files nested under
    # `drop_year_month=.../*.parquet` subdirectories, not the root, and
    # `listStatus` isn't recursive. `DESCRIBE DETAIL` already knows the
    # true file count/size regardless of partitioning structure.
    row = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    num_files = row["numFiles"]
    total_bytes = row["sizeInBytes"]
    return {
        "file_count": num_files,
        "total_mb": total_bytes / (1024 * 1024),
        "avg_file_kb": (total_bytes / num_files / 1024) if num_files else 0,
    }


def timed_query(spark, path: str, label: str) -> float:
    t0 = time.time()
    result = (
        spark.read.format("delta").load(path)
        .groupBy("store_id")
        .agg({"quantity_on_hand": "sum"})
        .collect()
    )
    elapsed = time.time() - t0
    print(f"  {label}: {len(result)} stores aggregated in {elapsed:.2f}s")
    return elapsed


def run() -> None:
    spark = get_spark("incident_08_compaction", shuffle_partitions=16)
    spark.sparkContext.setLogLevel("WARN")

    path = lakehouse_path("bronze", "pos_inventory_snapshots")

    print("=== before compaction ===")
    before_stats = file_stats(spark, path)
    print(f"  files: {before_stats['file_count']:,}  total: {before_stats['total_mb']:.1f}MB  "
          f"avg file size: {before_stats['avg_file_kb']:.1f}KB")
    before_time = timed_query(spark, path, "query wall time (before)")

    print("\n=== running OPTIMIZE (file compaction) ===")
    t0 = time.time()
    spark.sql(f"OPTIMIZE delta.`{path}`")
    print(f"  OPTIMIZE completed in {time.time() - t0:.1f}s")

    print("\n=== after compaction ===")
    after_stats = file_stats(spark, path)
    print(f"  files: {after_stats['file_count']:,}  total: {after_stats['total_mb']:.1f}MB  "
          f"avg file size: {after_stats['avg_file_kb']:.1f}KB")
    after_time = timed_query(spark, path, "query wall time (after)")

    print("\n=== summary ===")
    print(f"  file count   : {before_stats['file_count']:,} -> {after_stats['file_count']:,} "
          f"({100 * (1 - after_stats['file_count'] / before_stats['file_count']):.1f}% reduction)")
    print(f"  avg file size: {before_stats['avg_file_kb']:.1f}KB -> {after_stats['avg_file_kb']:.1f}KB")
    print(f"  query time   : {before_time:.2f}s -> {after_time:.2f}s")

    spark.stop()


if __name__ == "__main__":
    run()

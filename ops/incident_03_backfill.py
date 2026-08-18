"""
INCIDENT #3 (live demo): a month-long backfill with no double-counting.

Different failure mode than incident #2 (late-arriving fact). That one
proves a single new row gets picked up correctly; this one proves a
WHOLE historical partition can be wiped and rebuilt from bronze --
simulating a bad deploy that corrupted a month, or a genuine first-time
backfill of a month bronze already has but silver never processed --
without ever landing duplicate rows, no matter how many times the
rebuild is re-run.

Mechanism: DELETE the target month from silver.orders entirely (Delta's
own `.delete()`, a real transactional operation against the table, not
a simulation), then run the real silver MERGE job TWICE in a row against
unchanged bronze. Run 1 has to fully restore the month (bronze still has
every row; silver has none of them, so every row goes through
`whenNotMatchedInsertAll`). Run 2, against the now-restored silver, has
to be a complete no-op -- every row already matches, so it's pure
`whenMatchedUpdateAll` with unchanged values, zero inserts, zero
duplicates.

Run: `docker compose --profile orchestration exec airflow-scheduler
python /opt/airflow/ops/incident_03_backfill.py`
"""
from __future__ import annotations

import subprocess
import sys

TARGET_MONTH = "2025-03"  # a different month than incident #2's, to keep the two demos independent


def _spark_query(code: str) -> str:
    result = subprocess.run(
        ["python", "-c", code],
        cwd="/opt/airflow/spark_jobs",
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(result.stdout, file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError("spark query failed")
    return result.stdout


def month_count() -> int:
    out = _spark_query(f"""
from pyspark.sql import functions as F
from common.spark_session import get_spark, lakehouse_path
spark = get_spark('incident03_count')
spark.sparkContext.setLogLevel('WARN')
df = spark.read.format('delta').load(lakehouse_path('silver', 'orders'))
print('COUNT=' + str(df.filter(F.col('order_month') == '{TARGET_MONTH}').count()))
spark.stop()
""")
    for line in out.splitlines():
        if line.startswith("COUNT="):
            return int(line.split("=")[1])
    raise RuntimeError("no COUNT= line in output")


def distinct_vs_total_for_month() -> tuple[int, int]:
    out = _spark_query(f"""
from pyspark.sql import functions as F
from common.spark_session import get_spark, lakehouse_path
spark = get_spark('incident03_dupecheck')
spark.sparkContext.setLogLevel('WARN')
df = spark.read.format('delta').load(lakehouse_path('silver', 'orders')).filter(F.col('order_month') == '{TARGET_MONTH}')
print('TOTAL=' + str(df.count()))
print('DISTINCT=' + str(df.select('order_id').distinct().count()))
spark.stop()
""")
    total = distinct = None
    for line in out.splitlines():
        if line.startswith("TOTAL="):
            total = int(line.split("=")[1])
        if line.startswith("DISTINCT="):
            distinct = int(line.split("=")[1])
    return total, distinct


def delete_month_from_silver() -> None:
    _spark_query(f"""
from delta.tables import DeltaTable
from common.spark_session import get_spark, lakehouse_path
spark = get_spark('incident03_delete')
spark.sparkContext.setLogLevel('WARN')
t = DeltaTable.forPath(spark, lakehouse_path('silver', 'orders'))
t.delete("order_month = '{TARGET_MONTH}'")
spark.stop()
""")


def run_silver_merge(label: str) -> None:
    result = subprocess.run(
        ["python", "/opt/airflow/spark_jobs/bronze_to_silver/orders.py"],
        capture_output=True, text=True,
    )
    print(f"--- {label} output ---")
    print(result.stdout[-800:])
    if result.returncode != 0:
        print(result.stderr[-3000:], file=sys.stderr)
        raise RuntimeError(f"{label} failed")


def main() -> None:
    print(f"=== baseline: silver.orders for {TARGET_MONTH} ===")
    baseline = month_count()
    print(f"baseline count: {baseline:,}")
    if baseline == 0:
        raise RuntimeError(f"no orders found for {TARGET_MONTH} -- pick a month that actually has data")

    print(f"\n=== step 1: DELETE {TARGET_MONTH} entirely from silver.orders (simulating a corrupted/never-backfilled month) ===")
    delete_month_from_silver()
    after_delete = month_count()
    print(f"count after delete: {after_delete:,} (expected 0)")
    assert after_delete == 0, "delete didn't fully clear the target month"

    print(f"\n=== step 2: backfill run #1 (bronze still has every row; silver has none) ===")
    run_silver_merge("backfill run #1")
    after_run1 = month_count()
    print(f"count after run #1: {after_run1:,}")

    print(f"\n=== step 3: backfill run #2 (re-run the SAME merge against unchanged bronze) ===")
    run_silver_merge("backfill run #2")
    after_run2 = month_count()
    print(f"count after run #2: {after_run2:,}")

    total, distinct = distinct_vs_total_for_month()

    print("\n=== verification ===")
    print(f"baseline            : {baseline:,}")
    print(f"after run #1        : {after_run1:,}")
    print(f"after run #2        : {after_run2:,}")
    print(f"total rows (final)  : {total:,}")
    print(f"distinct order_ids  : {distinct:,}")

    assert after_run1 == baseline, f"run #1 should fully restore the month: expected {baseline:,}, got {after_run1:,}"
    assert after_run2 == after_run1, f"run #2 should be a no-op: expected {after_run1:,}, got {after_run2:,}"
    assert total == distinct, f"duplicate order_ids present: {total:,} total rows vs {distinct:,} distinct order_ids"

    print(f"\nPASS: {TARGET_MONTH} fully restored by run #1 ({after_run1:,} orders, matching the "
          f"{baseline:,}-row baseline exactly), run #2 was a true no-op, and every order_id in the "
          f"month is unique -- a full month backfill, re-run twice, with zero double-counting.")


if __name__ == "__main__":
    main()

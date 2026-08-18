"""
INCIDENT #5 (live demo): a quality gate that actually BLOCKS promotion,
not a test that reports after the fact.

Mechanism under test: airflow/dags/gold_promotion.py has exactly one
dependency -- `quality_gate >> dbt_build_gold`. Airflow's default
trigger rule (`all_success`) means a failed `quality_gate` task leaves
`dbt_build_gold` in `upstream_failed`, never executed. This script
proves that's real, not just configured:

  1. Deliberately corrupts silver.payments: appends a payment row whose
     amount does NOT reconcile with its order's order_items total --
     violating the exact invariant this dataset was designed to satisfy
     (see seeders/seed_postgres_oltp.py -- payments.amount_eur is
     DERIVED from order_items on purpose, specifically so this
     invariant is real and testable, not asserted about data that was
     never guaranteed to satisfy it).
  2. Triggers the real gold_promotion DAG via the Airflow CLI/DB (not a
     direct script call -- this exercises the actual orchestration).
  3. Verifies: quality_gate task state == failed, dbt_build_gold task
     state == upstream_failed (it never ran), and gold.fct_orders'
     row count is UNCHANGED from before the corrupt run (bad data never
     reached gold).
  4. Removes the corrupt row (silver is append-only in principle, but
     this is a deliberately-injected test artifact, not real pipeline
     output -- removing it is the equivalent of a real incident's
     "revert the bad deploy").
  5. Re-triggers gold_promotion, verifies both tasks succeed this time.

Run: `docker compose --profile orchestration exec airflow-scheduler
python /opt/airflow/ops/incident_05_quality_gate_block.py`
"""
from __future__ import annotations

import json
import subprocess
import sys
import time

BAD_PAYMENT_ID = 99_999_001


def _spark_query(code: str) -> str:
    result = subprocess.run(["python", "-c", code], cwd="/opt/airflow/spark_jobs", capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout, file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError("spark query failed")
    return result.stdout


def inject_bad_payment() -> None:
    # Explicit schema, matching silver.payments' real column types
    # exactly (amount_eur is decimal(5,2) -- a first version of this
    # injection used a plain Python float and a value that overflowed
    # that precision, which failed with DELTA_FAILED_TO_MERGE_FIELDS
    # before the demo even got to the interesting part). Building the
    # row via an explicit StructType sidesteps pandas/Spark type
    # inference entirely rather than hoping it lines up.
    _spark_query(f"""
from decimal import Decimal
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType, TimestampType
from common.spark_session import get_spark, lakehouse_path

schema = StructType([
    StructField("payment_id", LongType()),
    StructField("order_id", LongType()),
    StructField("payment_method", StringType()),
    StructField("amount_eur", DecimalType(5, 2)),
    StructField("payment_status", StringType()),
    StructField("paid_at", TimestampType()),
])

# Not going through the real ingestion pipeline on purpose -- this
# models a corrupt row reaching silver by whatever means (a bad
# manual fix, a bug in some other job), which is exactly the scenario
# the gate exists to catch regardless of how the bad row got there.
# 500.00 EUR against order_id=1's real (small, seeded) order_items
# total is a large, unambiguous mismatch -- comfortably inside
# decimal(5,2)'s range (max 999.99), unlike the first attempt.
row = [({BAD_PAYMENT_ID}, 1, "card", Decimal("500.00"), "captured", datetime.now(timezone.utc))]

spark = get_spark('inject_bad_payment')
spark.sparkContext.setLogLevel('WARN')
sdf = spark.createDataFrame(row, schema=schema)
sdf.write.format('delta').mode('append').save(lakehouse_path('silver', 'payments'))
spark.stop()
print("INJECTED")
""")


def remove_bad_payment() -> None:
    _spark_query(f"""
from delta.tables import DeltaTable
from common.spark_session import get_spark, lakehouse_path
spark = get_spark('remove_bad_payment')
spark.sparkContext.setLogLevel('WARN')
t = DeltaTable.forPath(spark, lakehouse_path('silver', 'payments'))
t.delete("payment_id = {BAD_PAYMENT_ID}")
spark.stop()
print("REMOVED")
""")


def fct_orders_row_count() -> int:
    out = _spark_query("""
import duckdb
con = duckdb.connect('/opt/airflow/data/warehouse/lakehouse.duckdb', read_only=True)
print('COUNT=' + str(con.execute('select count(*) from main_marts.fct_orders').fetchone()[0]))
""")
    for line in out.splitlines():
        if line.startswith("COUNT="):
            return int(line.split("=")[1])
    raise RuntimeError("no COUNT= line")


def trigger_and_wait(label: str, timeout_s: int = 240) -> dict:
    # -o json throughout: the default table output wraps long values
    # (like a `manual__<timestamp>` run_id) across two lines when the
    # terminal is narrow, which silently truncated the parsed run_id in
    # an earlier version of this script and made the whole demo hang
    # waiting on a DAG run that was never really being polled. JSON
    # output doesn't have a line-width concept to wrap around.
    trigger = subprocess.run(
        ["airflow", "dags", "trigger", "gold_promotion", "-o", "json"],
        capture_output=True, text=True,
    )
    # stdout has an INFO log line ahead of the actual JSON payload
    # ("Loaded API auth backend...") -- find the line that's actually
    # JSON rather than assuming stdout IS the JSON.
    run_id = None
    for line in trigger.stdout.splitlines():
        line = line.strip()
        if line.startswith("["):
            try:
                run_id = json.loads(line)[0]["dag_run_id"]
                break
            except Exception:
                continue
    if not run_id:
        print(trigger.stdout, file=sys.stderr)
        print(trigger.stderr, file=sys.stderr)
        raise RuntimeError("couldn't parse run_id from trigger output")

    print(f"[{label}] triggered {run_id}, waiting for completion...")
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        result = subprocess.run(
            ["airflow", "tasks", "states-for-dag-run", "gold_promotion", run_id, "-o", "json"],
            capture_output=True, text=True,
        )
        rows = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("["):
                try:
                    rows = json.loads(line)
                    break
                except Exception:
                    continue
        states = {r["task_id"]: r["state"] for r in rows if r.get("task_id") in ("quality_gate", "dbt_build_gold")}
        if states.get("quality_gate") in ("success", "failed") and states.get("dbt_build_gold") in (
            "success", "failed", "upstream_failed", "skipped",
        ):
            print(f"[{label}] final states: {states}")
            return states
        time.sleep(5)
    raise TimeoutError(f"[{label}] DAG run {run_id} did not finish within {timeout_s}s")


def main() -> None:
    baseline_gold_count = fct_orders_row_count()
    print(f"baseline gold.fct_orders row count: {baseline_gold_count:,}")

    print("\n=== step 1: inject a payment that breaks the reconciliation invariant ===")
    inject_bad_payment()

    print("\n=== step 2: trigger gold_promotion with corrupt silver data ===")
    states = trigger_and_wait("corrupt run")
    assert states.get("quality_gate") == "failed", f"expected quality_gate to fail, got {states.get('quality_gate')}"
    assert states.get("dbt_build_gold") == "upstream_failed", (
        f"expected dbt_build_gold to be upstream_failed (never run), got {states.get('dbt_build_gold')}"
    )

    gold_count_after_corrupt_run = fct_orders_row_count()
    print(f"gold.fct_orders row count after the BLOCKED run: {gold_count_after_corrupt_run:,}")
    assert gold_count_after_corrupt_run == baseline_gold_count, "gold changed despite the gate failing -- the block didn't work"
    print("CONFIRMED: gold was never touched -- dbt_build_gold never ran.")

    print("\n=== step 3: fix the corruption (remove the bad payment) ===")
    remove_bad_payment()

    print("\n=== step 4: re-trigger gold_promotion against clean silver data ===")
    states = trigger_and_wait("recovery run")
    assert states.get("quality_gate") == "success", f"expected quality_gate to pass, got {states.get('quality_gate')}"
    assert states.get("dbt_build_gold") == "success", f"expected dbt_build_gold to run and pass, got {states.get('dbt_build_gold')}"

    print("\nPASS: the gate blocked a real bad promotion (dbt never ran, gold never changed), "
          "and a clean re-run after the fix went through normally.")


if __name__ == "__main__":
    main()

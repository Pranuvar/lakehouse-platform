"""
SILVER -> GOLD quality gate. This is the piece the brief is explicit
about: "quality gates that block promotion, not tests that report after
the fact." The distinction that matters operationally: this script runs
and must PASS before the gold dbt build is ever invoked (see
airflow/dags/gold_promotion.py) -- a failure here means gold is never
touched, not "gold gets rebuilt with bad data and a test complains about
it afterward." Old gold stays exactly as it was: stale, but never wrong.

Chose a focused set of custom PySpark assertions over pulling in Great
Expectations for this. Not a knock on GX -- for a team running dozens of
tables and wanting a shared expectations-as-config UI/repository, it's
the right call. For 9 tables and a handful of genuinely load-bearing
invariants (the ones that would actually corrupt an aggregate if
violated), a plain assertion script is less machinery, easier to read
top to bottom, and just as enforceable from Airflow -- same "don't build
an abstraction with one caller" reasoning as ingestion/'s pipeline
metadata (see docs/BUILD_LOG.md).

Every check here is something this project already cares about
elsewhere (the payments/order_items reconciliation was designed INTO the
seed data on purpose -- see seeders/seed_postgres_oltp.py); this is
where that invariant gets actually enforced as a gate, not just observed.

Exit code 0 = every check passed, gold may proceed. Exit code 1 = at
least one check failed; the full report is printed either way.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass

from pyspark.sql import functions as F

from common.spark_session import get_spark, lakehouse_path

RECONCILIATION_TOLERANCE_EUR = 0.05


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def check_payments_reconcile(spark) -> CheckResult:
    items = spark.read.format("delta").load(lakehouse_path("silver", "order_items"))
    payments = spark.read.format("delta").load(lakehouse_path("silver", "payments"))

    item_totals = items.groupBy("order_id").agg(
        F.sum(F.col("quantity") * F.col("unit_price_eur") * (1 - F.col("discount_pct") / 100.0)).alias("item_total")
    )
    payment_totals = payments.groupBy("order_id").agg(F.sum("amount_eur").alias("paid_total"))

    mismatched = (
        item_totals.join(payment_totals, "order_id")
        .where(F.abs(F.col("item_total") - F.col("paid_total")) > RECONCILIATION_TOLERANCE_EUR)
    )
    n_bad = mismatched.count()
    if n_bad > 0:
        sample = mismatched.limit(5).collect()
        detail = f"{n_bad:,} orders where sum(order_items) != sum(payments) beyond {RECONCILIATION_TOLERANCE_EUR} EUR tolerance. Sample: {sample}"
    else:
        detail = "every order's payments reconcile to its order_items total"
    return CheckResult("payments_reconcile_to_order_items", n_bad == 0, detail)


def check_no_duplicate_orders(spark) -> CheckResult:
    orders = spark.read.format("delta").load(lakehouse_path("silver", "orders"))
    total = orders.count()
    distinct = orders.select("order_id").distinct().count()
    passed = total == distinct
    detail = f"{total:,} total rows, {distinct:,} distinct order_id" if not passed else f"{total:,} orders, all order_id unique"
    return CheckResult("no_duplicate_order_ids", passed, detail)


def check_no_orphaned_child_rows(spark) -> CheckResult:
    orders = spark.read.format("delta").load(lakehouse_path("silver", "orders")).select("order_id")
    orphan_counts = {}
    for table in ["order_items", "payments"]:
        child = spark.read.format("delta").load(lakehouse_path("silver", table))
        n_orphans = child.join(orders, "order_id", "left_anti").count()
        orphan_counts[table] = n_orphans
    total_orphans = sum(orphan_counts.values())
    detail = ", ".join(f"{t}: {n:,} orphaned" for t, n in orphan_counts.items())
    return CheckResult("no_orphaned_order_references", total_orphans == 0, detail)


def check_pos_inventory_parse_quality(spark) -> CheckResult:
    """
    Deliberately checks TRUE parse failures, not "how many nulls are in
    the clean column" -- those aren't the same thing, and conflating
    them is a bug this check itself had at first. bronze.pos_inventory_
    snapshots has ~2% genuinely NULL quantity_on_hand baked into the
    seed data on purpose (simulating till-export truncation -- see
    seeders/seed_flatfiles.py's `blank_mask`), which silver correctly
    preserves as NULL rather than fabricating a value. An earlier
    version of this check counted silver nulls directly and failed the
    gate on that -- a real, working data pipeline, block for the wrong
    reason. The property that actually indicates a broken parser is
    "raw value was present, but nothing numeric could be extracted from
    it" -- checked here by re-joining to bronze, exactly the same
    condition `pos_inventory.py`'s own diagnostic already computes at
    build time; this is that same check enforced as a gate, not a new
    one.
    """
    bronze = spark.read.format("delta").load(lakehouse_path("bronze", "pos_inventory_snapshots"))
    total = bronze.count()

    has_unit_cost = "unit_cost" in bronze.columns
    unit_cost_raw = F.coalesce(F.col("unit_cost_eur"), F.col("unit_cost")) if has_unit_cost else F.col("unit_cost_eur")
    NUMERIC_PATTERN = r"(\d+\.?\d*)"

    cost_extracted = F.regexp_extract(unit_cost_raw, NUMERIC_PATTERN, 0)
    qty_extracted = F.regexp_extract(F.col("quantity_on_hand"), NUMERIC_PATTERN, 0)

    n_unparseable_cost = bronze.filter(unit_cost_raw.isNotNull() & (cost_extracted == "")).count()
    n_unparseable_qty = bronze.filter(F.col("quantity_on_hand").isNotNull() & (qty_extracted == "")).count()
    n_source_null_qty = bronze.filter(F.col("quantity_on_hand").isNull()).count()

    n_bad = n_unparseable_cost + n_unparseable_qty
    passed = n_bad == 0
    detail = (
        f"true parse failures: unit_cost_eur={n_unparseable_cost:,}, quantity_on_hand={n_unparseable_qty:,} "
        f"(out of {total:,} bronze rows) -- genuinely source-missing quantity_on_hand (not a parse failure, "
        f"informational only): {n_source_null_qty:,} ({n_source_null_qty/total:.2%})"
    )
    return CheckResult("pos_inventory_no_true_parse_failures", passed, detail)


def check_no_duplicate_customer_identities(spark) -> CheckResult:
    customers = spark.read.format("delta").load(lakehouse_path("silver", "customers"))
    total = customers.count()
    distinct_emails = customers.select("email").distinct().count()
    passed = total == distinct_emails
    detail = f"{total:,} customer rows, {distinct_emails:,} distinct emails" if not passed else f"{total:,} customers, identity resolution clean"
    return CheckResult("no_duplicate_customer_identities", passed, detail)


def check_campaign_performance_natural_key(spark) -> CheckResult:
    df = spark.read.format("delta").load(lakehouse_path("silver", "campaign_performance"))
    total = df.count()
    distinct = df.select("campaign_id", "ad_set_id", "date").distinct().count()
    passed = total == distinct
    detail = f"{total:,} rows, {distinct:,} distinct (campaign_id, ad_set_id, date)" if not passed else f"{total:,} rows, natural key clean"
    return CheckResult("campaign_performance_natural_key_unique", passed, detail)


CHECKS = [
    check_payments_reconcile,
    check_no_duplicate_orders,
    check_no_orphaned_child_rows,
    check_pos_inventory_parse_quality,
    check_no_duplicate_customer_identities,
    check_campaign_performance_natural_key,
]


def run() -> bool:
    spark = get_spark("silver_to_gold_quality_gate")
    spark.sparkContext.setLogLevel("WARN")

    results = [check(spark) for check in CHECKS]
    spark.stop()

    print("\n" + "=" * 70)
    print("SILVER -> GOLD QUALITY GATE")
    print("=" * 70)
    all_passed = True
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.name}")
        print(f"       {r.detail}")
        if not r.passed:
            all_passed = False
    print("=" * 70)
    if all_passed:
        print("GATE PASSED -- gold promotion may proceed.")
    else:
        print("GATE FAILED -- gold promotion BLOCKED. Gold tables will not be touched.")
    print("=" * 70 + "\n")

    return all_passed


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)

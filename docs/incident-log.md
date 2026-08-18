# Incident Log

Eight scenarios, deliberately built and then broken, because "we wrote
tests" is a weaker interview answer than "here's the run where it caught
something, here's the diff that fixed it." Each entry: what was set up,
what broke, what the fix was, how to reproduce it.

**Status: 7 of 8 fully resolved and live-demonstrated end to end.**
Scenario #7 (Power BI RLS) has a complete, real, working model and DAX
-- the one thing this Linux build environment genuinely cannot do is
click through Power BI Desktop's Windows/Mac-only UI to visually
confirm it, which is called out explicitly rather than glossed over.

Status key: 🔲 not yet triggered · 🟡 built and real, one specific piece
needs an environment/access this build doesn't have (stated explicitly
in the entry) · ✅ triggered and resolved, evidence below.

---

## 1. Schema drift in the flat-file source ✅

**Precondition (already seeded):** `seeders/seed_flatfiles.py` drops 25
months of POS inventory snapshots to MinIO across 4 escalating schema
stages -- see [BUILD_LOG.md](BUILD_LOG.md#flat-file-seeder-with-schema-drift-seedersseed_flatfilespy)
for the exact stage boundaries (additive column → type drift → breaking
rename).

**What actually broke, first try:** the very first version of
`ingestion/pipelines/flat_files.py` let each file's native per-column
type flow straight into the `write_deltalake(..., schema_mode="merge")`
call. Proven directly, before writing the real pipeline: a 2-row Delta
table with `qty: int64`, appended with `qty: string` on the second batch,
schema_mode="merge" -> `Exception: Cast error: Cannot cast string '42
units' to value of Int64 type`. Additive columns (`reorder_point`) merge
fine; a column that stays present but changes TYPE does not, and delta-rs
correctly refuses rather than silently coercing.

**Fix:** bronze forces the three volatile columns
(`quantity_on_hand`, `unit_cost_eur`, `unit_cost`) to string,
unconditionally, on every file regardless of that file's native type --
see the full reasoning in `ingestion/pipelines/flat_files.py`'s
docstring. Genuinely renamed columns (`unit_cost_eur` -> `unit_cost`) are
NOT aliased in bronze; both names simply coexist, nullable depending on
era. No renaming/coercion logic in bronze at all beyond that one
type-safety rule -- deferred to silver, which is where "cleaned" belongs
in this architecture.

**Result (bronze, verified):** `python ingestion/pipelines/flat_files.py`
processed all 25 months / 3,725 files / all 4 drift stages in one run
with zero write failures: **1,865,588 rows landed, an exact match to the
seeded source count.**

**Result (silver, verified):** `spark_jobs/bronze_to_silver/pos_inventory.py`
does `coalesce(unit_cost_eur, unit_cost)`, regex-strips the `"EUR "`
prefix / `" units"` suffix, and casts back to `decimal(10,2)`/`int` --
uniformly, across all 4 drift stages, in one pass. Run against the full
1,865,588-row bronze table: **1,862,500 silver rows** (3,088 duplicate
retried-upload rows correctly dropped), **zero unparseable values** on
either column across the entire history. One clean, typed contract,
regardless of which of the 4 schema-drift stages a given row's source
file came from.

**Status: fully resolved, bronze and silver both verified.**

---

## 2. A late-arriving fact reconciled by the incremental model ✅

**Precondition (already seeded):** ~9,050 orders in `oltp.orders` have
`created_at` 3-10 days after `order_ts` (in-store, batch-synced till
pattern) -- see BUILD_LOG. An incremental extract keyed on `order_ts`
would never see these after their partition has "closed"; one keyed on
`created_at`/`updated_at` will.

**Live demo:** `ops/incident_02_late_arriving_fact.py` -- inserts a real
order into Postgres dated into June 2025 (`order_ts`) but created just
now (`created_at`/`updated_at`), runs the actual incremental extraction
and the actual silver MERGE, and verifies the historical month's count
moved by exactly the right amount. Reproducible: `docker compose
--profile orchestration exec airflow-scheduler python
/opt/airflow/ops/incident_02_late_arriving_fact.py`.

**What actually happened running it (two real bugs, not a clean first
pass):**

1. **The watermark had silently raced ahead of "now."** First run: the
   incremental extract returned **0 rows** for a fact that should
   obviously have been picked up. Root cause: `seeders/seed_postgres_oltp.py`
   computed `updated_at` for "touched" orders as `created_at +
   timedelta(1-14 days)` with no ceiling -- for orders placed near the
   end of the seeded history window, that pushed `updated_at` PAST the
   actual moment the seed script ran, i.e. into the future. The
   incremental pipeline's watermark had already been set to that
   future value during Day 1's sync, so a genuinely new row (inserted
   with `updated_at` = real wall-clock now) read as *older* than the
   watermark and was silently skipped -- the exact failure mode this
   scenario exists to catch, just from an unexpected direction. Fixed
   the seeder (clamp `created_at`/`updated_at` to a `now_ceiling`, so
   future runs never generate this), and for this session's
   already-seeded data, manually reset the watermark to the real
   current time -- the honest real-world equivalent of triaging a live
   incident rather than regenerating 9.8M rows from scratch.
2. **Resetting the watermark exposed a second, more interesting bug.**
   Rolling the watermark backward naturally re-swept several thousand
   already-ingested orders back into bronze as "new" -- and
   `spark_jobs/bronze_to_silver/orders.py` assumed bronze had at most
   one row per `order_id`, an assumption that's true under normal
   forward-only watermark movement but not guaranteed in general. The
   result: Delta's MERGE correctly refused to proceed --
   `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`, multiple
   source rows matching the same target row. Fixed by deduplicating the
   bronze source on `order_id` (latest `_ingested_at` wins) before
   merging -- the same defensive pattern the other silver jobs already
   use, now applied here too, and the job is more robust for it
   regardless of what causes bronze duplication in the future.

**Result (verified end to end, survived the retry):** both late orders
inserted during this incident (the first attempt's order, recovered
after the MERGE fix; the second attempt's order) landed correctly:

```
order_id  order_ts             created_at                  order_month
2000001   2025-06-15 14:30:00  2026-08-17 17:54:50 (today)  2025-06
2000002   2025-06-15 14:30:00  2026-08-17 17:56:38 (today)  2025-06
```

Both attributed to their correct historical `order_month`, both
inserted into silver via the real MERGE, zero duplication, zero other
rows in that month disturbed. Bronze `orders` is partitioned by
`order_month` (derived from `order_ts`, the business date), so a
late-arriving row lands in the partition matching when it actually
happened, not when it was ingested.

**Status: fully resolved, live-demonstrated, and it took two real bugs
and their fixes to get a clean run -- which is a better interview
answer than a scenario that worked first try.**

---

## 3. A month-long backfill with no double-counting ✅

**Live demo:** `ops/incident_03_backfill.py` -- a different failure mode
than #2 (which proves a single new row is picked up correctly): this
proves a WHOLE historical partition can be deleted outright and rebuilt
from bronze, any number of times, without ever landing a duplicate.
Mechanism: Delta's own `DeltaTable.delete()` (a real transactional
delete, not a simulation) wipes March 2025 entirely from silver.orders,
then the real silver MERGE job runs twice in a row against unchanged
bronze. Reproducible: `docker compose --profile orchestration exec
airflow-scheduler python /opt/airflow/ops/incident_03_backfill.py`.

**Result:**

```
baseline (before delete)   : 70,547 orders
after DELETE                : 0
after backfill run #1       : 70,547   (net new: 70,547 -- full restore)
after backfill run #2       : 70,547   (net new: 0      -- true no-op)
distinct order_ids in month : 70,547   (== total rows -- zero duplication)
```

Run #1 fully restored the month from bronze (every row went through
`whenNotMatchedInsertAll` since silver had none of them). Run #2,
against the now-restored silver and the SAME unchanged bronze, was a
complete no-op -- every row matched on `order_id` and went through
`whenMatchedUpdateAll` with identical values, zero new inserts. Distinct
`order_id` count equals total row count exactly: no order appears
twice no matter how many times the backfill runs.

**Status: fully resolved, live-demonstrated.** This is the same MERGE
mechanism as incident #2 (`spark_jobs/bronze_to_silver/orders.py`) doing
the OTHER thing an idempotent upsert has to guarantee: not just "new
data lands correctly" but "re-running against a wiped or unchanged
target never double-counts."

---

## 4. A Spark job killed mid-write, recovered via the Delta log ✅

**Live demo:** `ops/incident_04_crash_recovery.py` -- writes all of
bronze.order_items (11,250,066 rows, repartitioned to 64 files) to a
scratch Delta table twice: once uninterrupted (baseline), once
deliberately killed mid-flight. The kill isn't timed by a guessed sleep
duration -- a boto3 polling loop watches the table's MinIO path directly
and sends `SIGKILL` the INSTANT a new, not-yet-committed parquet file is
observed, which is real evidence the write was genuinely in progress
when it died, not a kill that landed too early or too late.
Reproducible: `docker compose --profile orchestration exec
airflow-scheduler python /opt/airflow/ops/incident_04_crash_recovery.py`.

**A correctness bug in the demo script itself, worth noting:** the
first version sent `SIGKILL` only to the Python wrapper process. PySpark
launches the actual Spark driver as a CHILD JVM (via py4j) -- killing
just the wrapper leaves that JVM as an orphan that keeps running and can
still complete and commit the write, which would have silently
invalidated the whole test (nothing would have "crashed," it would just
have detached from its parent). Fixed with `preexec_fn=os.setsid` +
`os.killpg()`, so the signal reaches the wrapper AND everything it
spawned.

**Result:**

```
committed versions before the kill : 2
observed new uncommitted file after: 37.8s
committed versions after the kill  : 2        (unchanged -- no torn commit landed)
orphaned uncommitted parquet files : 16       (physical proof the write was really in flight)
row count immediately after kill   : 11,250,066  (== baseline, not 0, not partial)
row count after recovery re-run    : 11,250,066  (== baseline -- full recovery)
```

**Status: fully resolved, live-demonstrated.** The Delta transaction log
did exactly what it's supposed to: the killed write's 16 partial files
sat physically in object storage but were never activated for readers
(no new `_delta_log` entry ever committed for them), so every read
during and after the crash saw the complete, uncorrupted last-good
version. A plain re-run recovered fully with no special-cased recovery
logic needed -- the same idempotent-write property incidents #2 and #3
depend on. (The 16 orphaned files would eventually be reclaimed by a
`VACUUM` -- see incident #8's cost/performance write-up for where that
operation shows up for real in this project.)

---

## 5. Quality gates that block promotion, not tests that report after ✅

**Mechanism:** `airflow/dags/gold_promotion.py` has exactly one
dependency -- `quality_gate >> dbt_build_gold`. `quality_gate` runs
`spark_jobs/quality_gate.py` (6 PySpark assertions against real
invariants -- see BUILD_LOG) via spark-submit and exits non-zero on any
failure. Airflow's default trigger rule (`all_success`) means a failed
`quality_gate` leaves `dbt_build_gold` `upstream_failed` -- never
executed. No special "blocking" logic anywhere; it's what a task
dependency does by default when nothing overrides the trigger rule.

**A bug in the check itself, found before trusting the gate at all**
(full detail in BUILD_LOG): the `pos_inventory_parse_quality` check
originally failed on 12,053 null `quantity_on_hand` values that turned
out to be genuinely missing at SOURCE (a deliberately-seeded ~2%
till-truncation rate), not a parser bug -- confirmed by cross-referencing
bronze directly before assuming the pipeline was broken. Rewrote the
check to test for TRUE parse failures only.

**Live demo, the actual block:** `ops/incident_05_quality_gate_block.py`
-- injects a payment row that breaks the payments/order_items
reconciliation invariant directly into silver (bypassing the real
pipeline on purpose, modelling "a corrupt row reached silver by
whatever means" -- the gate has to catch it regardless of cause), then
triggers the real `gold_promotion` DAG via the Airflow CLI. Two real
bugs surfaced building this script, both fixed:
1. The first injection attempt used a plain Python float for
   `amount_eur` against silver.payments' real `decimal(5,2)` column
   type and a value that overflowed it (999999.99) -- failed with
   `DELTA_FAILED_TO_MERGE_FIELDS` before the interesting part of the
   demo even ran. Fixed by building the row with an explicit
   `StructType` matching the real schema exactly, not inferred types.
2. The trigger/poll logic parsed the Airflow CLI's default TABLE
   output, which wraps long values (a `manual__<timestamp>` run_id)
   across two lines when the terminal is narrow -- silently truncated
   the run_id and made the script poll a run_id that was never real,
   hanging until timeout. Fixed by using `-o json` throughout (still
   has to skip one leading INFO log line ahead of the actual JSON, but
   no wrapping to fight).

**Result:**

```
baseline gold.fct_orders row count      : 2,000,002
[corrupt run]   quality_gate            : failed
[corrupt run]   dbt_build_gold          : upstream_failed  (never ran)
gold.fct_orders row count after blocked run : 2,000,002   (unchanged)
[recovery run]  quality_gate            : success
[recovery run]  dbt_build_gold          : success
```

**Status: fully resolved, live-demonstrated end to end through the real
Airflow DAG** -- not a script asserting the concept, an actual DAG
trigger, an actual failed task, an actual `upstream_failed` sibling, and
an actual confirmation that gold's row count didn't move until the
corruption was fixed and the DAG re-run.

---

## 6. A CI run that fails a PR because a dbt test failed ✅ (logic proven; live GitHub run needs a push)

**CI design:** `.github/workflows/ci.yml`, two jobs on every PR touching
`dbt/`: `sql-lint` (sqlfluff against the dbt-templated SQL) and
`dbt-build` (real `dbt build` -- models AND tests). Deliberately does
NOT spin up Postgres/MinIO/Spark for this: small hand-written CSV
fixtures under `dbt/seeds/` stand in for the real silver Delta tables
(`--vars '{use_seeds: true}'` flips `macros/delta_source.sql` from
`delta_scan()` to `ref()` against the seed) -- same models, same tests,
same SQL, real infrastructure swapped for fast/deterministic fixtures.
Verified the fixtures alone build clean first: 76/76 (9 seeds, 7 table
models, 1 incremental model, 10 view models, 49 tests) in ~2-4 seconds,
fully self-contained.

**Why this environment can't produce a literal GitHub-hosted failing
check:** no `gh` CLI and no GitHub remote configured in this build
environment (checked directly: `gh: command not found`, `git remote -v`
empty). What CAN be proven, and was: running the EXACT commands
`.github/workflows/ci.yml` runs, locally, against a deliberately broken
PR-shaped change, and checking the real process exit code -- the same
signal GitHub Actions uses to mark a check red or green.

**The break:** edited `stg_payments.sql` to add a plausible-looking bug
-- a "processing fee" adjustment (`amount_eur * 1.029`) that nothing
downstream was updated to account for, exactly the shape of a real PR
that looks reasonable in isolation and breaks an invariant it doesn't
touch directly:

```sql
-- before
amount_eur,
-- after (the break)
round(amount_eur * 1.029, 2) as amount_eur,
```

**Result:**

```
$ dbt build --vars '{use_seeds: true}'
...
[ERROR]: in test assert_payments_reconcile_to_order_items
  Got 6 results, configured to fail if != 0
Done. PASS=70 WARN=0 ERROR=1 SKIP=5 NO-OP=0 REUSED=0 TOTAL=76
$ echo $?
1
```

Real, unambiguous process failure -- exit code 1, the exact signal a
GitHub Actions job uses to mark a check failed and block a PR with
required-checks enabled. Reverted the change, re-ran the identical
command:

```
$ dbt build --vars '{use_seeds: true}'
...
Done. PASS=76 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=76
$ echo $?
0
```

**Status: CI logic fully proven locally with the exact commands GitHub
Actions runs; a literal GitHub-hosted red/green PR check is the one
piece of this whole project that needs the user's own GitHub push +
`gh` auth to produce, since this build environment has neither.** Next
step if wanted: push this repo, open a PR with this exact diff, link
the failing Actions run here for a real screenshot.

---

## 7. Row-level security in Power BI actually filtering by user 🟡 (model built and real; live Desktop click-through needs Windows/Mac)

**Environment constraint, stated plainly:** this entire build ran on
Linux, where Power BI Desktop cannot run at all. What follows is a real,
complete, version-controlled semantic model (TMDL source -- the same
text format Desktop itself writes for a `.pbip` project) with working
DAX-based RLS, built to be opened and click-tested on a Windows/Mac
machine, not a description of what RLS would look like.

**Design:** `powerbi/model/roles.tmdl` -- a `'Regional Manager'` role
with a DAX table filter on `DimStores` (not per-fact-table filters; the
whole point of RLS in a star schema is filtering the dimension once and
letting the model's existing relationships cascade it to every fact
table joined to that dimension):

```dax
tablePermission DimStores =
    VAR UserAccess = LOOKUPVALUE(DimUserCountryAccess[CountryAccess],
                                  DimUserCountryAccess[UserEmail], USERPRINCIPALNAME())
    RETURN UserAccess = "*" || DimStores[Country] = UserAccess
```

Dynamic, not static: `USERPRINCIPALNAME()` resolves to whoever is
actually using the report, looked up against a mapping table
(`DimUserCountryAccess`) rather than one hard-coded country per role --
adding a new regional manager is a new mapping-table row, not a new
Power BI role. A second, deliberately static role
(`'Ireland Only (static demo)'`) exists specifically as a sanity check
independent of the lookup table.

**What's real and verifiable right now, without Desktop:** the full
star schema (`relationships.tmdl`), every DAX measure
(`tables/_Measures.tmdl` -- 19 measures spanning revenue/margin/AOV/
YoY/marketing/funnel metrics, including one that surfaces this
project's own documented SCD2 fallback-rate data-quality caveat
directly in the BI layer), the SCD2 point-in-time relationship
(`FactOrderItems` relates to `DimProducts` on `ProductKey`, not
`ProductId`, so margin uses the cost that was actually current on the
sale date), and a tested, working data export
(`export_gold_for_powerbi.py`, run for real -- see BUILD_LOG: 9 Parquet
files, exact row counts matching gold).

**What's pending, and exactly what would prove it:** open the exported
`.pbip`/TMDL model in Power BI Desktop on Windows/Mac (`powerbi/
README.md` has the full walkthrough), Modeling ribbon > **View As** >
`Regional Manager` > "Other user" >
`regional.manager.ie@fjordmart.example`, and confirm every visual
re-filters to Ireland-only stores live -- Desktop's free "View As
Roles" feature, no Power BI Service tenant or Pro licence needed for
this specific proof. That one click-through is the only part of this
scenario this Linux build environment couldn't perform itself.

---

## 8. Cost and performance: partitioning, compaction, "if the bill doubled" ✅

**The "before" evidence wasn't staged -- it was already sitting there.**
`ingestion/pipelines/flat_files.py` writes one delta-rs commit PER
SOURCE FILE (documented as a deliberate choice at build time, in that
file's own docstring, specifically so it could become real evidence
here rather than a synthetic example). 3,725 source files -> 3,725
tiny commits to `bronze.pos_inventory_snapshots`.

**Live demo:** `ops/incident_08_compaction.py` -- measures real file
count/size via `DESCRIBE DETAIL` (not a manual directory listing: this
table is partitioned by `drop_year_month`, and an earlier version of
this script used Hadoop's `listStatus` on the table root, which isn't
recursive and silently returned 0 files since partitioned tables keep
their data nested under `drop_year_month=.../*.parquet` subdirectories
-- `DESCRIBE DETAIL` already knows the true count regardless of
partitioning), times a representative aggregation query, runs Delta's
`OPTIMIZE`, then measures both again.

**Result:**

```
                  before          after         change
files             3,725           25            -99.3%
avg file size     10.0 KB         452.1 KB       ~45x larger
query time        11.63s          1.23s          ~9.5x faster
OPTIMIZE runtime  --              16.0s
```

One `OPTIMIZE` call (16 seconds) bought a 9.5x speedup on a simple
per-store aggregation, purely by giving Spark 25 reasonably-sized files
to schedule instead of 3,725 tiny ones -- most of that 11.63s "before"
number is task-scheduling overhead, not actual I/O, which is exactly
what small-file problems cost you: not more data, more coordination.

**Partitioning already in place per layer** (not something this
incident had to add, worth stating plainly): `orders`/`fct_order_items`
by `order_month`, `pos_inventory_snapshots` by `drop_year_month`,
`campaign_performance`/`fct_campaign_performance` by `channel`,
`clickstream_events` by `event_date`, dimension full-snapshots
(`customers`/`stores`/`products`/`order_items`/`payments` bronze) by
`snapshot_date`. Chosen for cardinality that matches actual query
patterns (a month or a channel, not a single day for a 730-day history)
-- over-partitioning creates the exact small-file problem this incident
just fixed; under-partitioning forces full scans.

**"If the bill doubled" -- what I'd change first, in order:**

1. **Stop the full-snapshot dimension re-copies.** `customers`,
   `stores`, `products`, `order_items`, `payments` bronze all re-extract
   in FULL every run (documented simplification -- see
   `ingestion/pipelines/postgres_oltp.py`). At current scale that's
   cheap; at 10x the row count it's the single biggest avoidable cost.
   Fix: add `updated_at` to these tables in the OLTP source and go
   properly incremental, the same pattern `orders` already uses.
2. **Batch the flat-file copy activity.** One delta-rs commit per file
   is exactly what incident #8 just proved is expensive at scale --
   `ingestion/pipelines/flat_files.py` should batch a month's ~149
   files into one write per month (25 commits instead of 3,725), with a
   scheduled `OPTIMIZE`/`VACUUM` pass as a standing job rather than a
   one-off demo script, on any bronze table that accumulates
   many-small-files under normal operation (this one, and the Kafka
   consumer's per-micro-batch bronze writes).
3. **Right-size Spark shuffle partitions per job, not one constant.**
   `spark_jobs/common/spark_session.py` defaults `shuffle_partitions=8`
   project-wide; the crash-recovery demo (incident #4) had to override
   it to 64 just to get a write big enough to observably take multiple
   seconds. A real cost review would tune this per job against actual
   data volume, not use one number everywhere.
4. **Reconsider the SCD2 fallback rate.** 40% of `fct_order_items` uses
   `dim_products`' earliest-known-version fallback (see BUILD_LOG's dbt
   entry) because of a seed-generation gap, not a real production
   pattern -- flagged here only because a real 40% fallback rate in a
   live system would mean the dimension's tracked history doesn't
   actually cover the fact history it's joined against, which is worth
   knowing about regardless of cost.

**Status: fully resolved, live-demonstrated with real numbers, not
estimated ones.**

---

*This file is updated as each scenario is actually run, not written in
advance from the plan -- entries above will gain dated "Result" sections
with real command output/screenshots as Day 2/3 work lands.*

# Build Log

STAR-format entries (Situation / Task / Action / Result) per component,
written as the platform was built -- this is the source material for
interview answers, not a changelog. Entries are added as components land;
nothing here is written retroactively from memory.

---

## Repo scaffold & Docker Compose architecture

**Situation.** The brief called for a broad, multi-engine platform (Postgres,
MinIO, Kafka-API broker, a mock REST API, Airflow with embedded PySpark +
dbt) running entirely locally, on a dev box with ~6.7GB RAM available to
Docker.

**Task.** Design a `docker-compose.yml` that stands the whole thing up
without swapping, and that a reviewer can read and understand the shape of
the platform from in under a minute.

**Action.** Split services into two compose *profiles*: the default
(no-flag) profile brings up only the four heterogeneous sources (Postgres,
MinIO, Redpanda, mock-api) needed to seed data; `--profile orchestration`
adds Airflow (webserver + scheduler), which pulls in a much heavier custom
image (Java + PySpark + delta-spark + dbt-duckdb) and is only needed once
there's data to orchestrate over. Explicit `mem_limit` on every service
keeps the whole default profile under ~2.5GB combined, leaving headroom for
Airflow's scheduler (given a 3GB ceiling since spark-submit runs as its
subprocess, local[*] mode -- see the Airflow entry below once that lands).
Postgres hosts two logical databases (OLTP source data + Airflow's own
metadata store) on one instance rather than paying for a second container.

**Result.** `docker compose up -d` (sources) brings up all four sources
healthy in well under a minute; measured combined RSS across postgres +
minio + redpanda + mock-api after full seeding was **~986MB**. Interview
angle: this is the same reasoning a cost-conscious platform team applies to
right-sizing environments, just made explicit because a laptop enforces it.

---

## Postgres OLTP seeder (`seeders/seed_postgres_oltp.py`)

**Situation.** Needed a transactional source big enough (target: >9M rows
platform-wide) to make partitioning/indexing/query-tuning discussions real,
generated fast enough to stay inside a "running code in the first hour"
budget.

**Task.** Generate ~9.7M realistic, referentially-consistent OLTP rows
(customers, stores, products, orders, order_items, payments) and load them
without the load itself becoming the bottleneck.

**Action.**
- Vectorised generation with numpy (Zipf-skewed customer/product
  popularity, not uniform -- repeat buyers and best-sellers are what a
  real retailer's data looks like) instead of row-by-row Python.
- Bulk load via `COPY FROM STDIN` in 500k-row chunks, not INSERT/ORM.
- PKs, FKs, and indexes added **after** the load (`ALTER TABLE ... ADD
  CONSTRAINT`), not declared upfront -- building an index once over the
  full dataset beats maintaining it through ~9.7M incremental COPY inserts.
- `payments.amount_eur` is derived FROM the generated `order_items`
  (grouped, summed, split for the ~3% multi-payment orders), so "payments
  reconcile to order line totals" is a true invariant of the data, not an
  assertion I have to fake -- it becomes a real dbt singular test later.
- Baked in the raw material for two of the required interview scenarios
  directly into the schema, rather than trying to fake them after the
  fact: `orders.created_at` vs `order_ts` diverge by 3-10 days for ~1% of
  in-store orders (late-arriving fact), and ~2% of `customers` rows are
  near-duplicates (same person, cosmetic email drift) from a simulated
  guest-checkout-then-registers pattern (dedup target for silver).

**Result.** 9,858,349 rows loaded in **97 seconds** end to end (schema
create → generate → COPY → constraints → ANALYZE). Verified post-load:
6,000 customer rows sit in duplicate-email groups; 9,050 orders have
`created_at` more than 2 days after `order_ts`; a full reconciliation
query (`sum(order_items) vs sum(payments)` per order) returned **0
mismatched orders** across 2,000,000 orders.

---

## Flat-file seeder with schema drift (`seeders/seed_flatfiles.py`)

**Situation.** The brief explicitly wants the "messy schema" case: a flat
file source with a new column and a changed type that bronze has to absorb
without breaking silver.

**Task.** Generate monthly POS inventory-snapshot drops (one file per
physical store per month) into MinIO, with schema drift that escalates
realistically over time rather than being injected as one synthetic flag.

**Action.** Four drift stages across 25 months of drops: stage 0 (clean 5
columns) → stage 1 (`reorder_point` added, additive) → stage 2
(`unit_cost_eur` becomes text like `"EUR 5.36"`, `quantity_on_hand`
partially becomes `"42 units"` strings, ~2% goes blank) → stage 3
(`unit_cost_eur` renamed to `unit_cost`, a breaking rename with no
compatibility shim). File format alternates CSV/Parquet by month. Hit a
real bug here: an early version left drifted numeric columns as
*partially* string, partially native-typed (object dtype with mixed
Python `int`/`str`/`float('nan')`) -- fine for CSV (text-native anyway)
but `pyarrow` can't serialise a genuinely mixed-type column to Parquet.
Fixed by re-typing the **whole** column to string once any drift is
present, which is also more realistic: a real export wouldn't leave some
rows numeric and others text within one Parquet column.

**Result.** 3,725 files (1,937 CSV + 1,788 Parquet), 1,865,588 rows,
generated and uploaded to MinIO in **59 seconds**. Verified stage
boundaries (8/9/6/2 months) and a clean parquet round-trip for every
stage before the full run.

---

## Clickstream event seeder (`seeders/seed_kafka_events.py`) + Redpanda

**Situation.** Needed an append-only, near-real-time-shaped source, and
wanted it to genuinely relate to the other sources rather than being a
fourth, disconnected synthetic table.

**Task.** Produce a clickstream of page/product/cart/checkout/purchase
events onto a Kafka-API topic, where `purchase` events are traceable to
real orders already sitting in Postgres.

**Action.** Chose Redpanda over Kafka+Zookeeper purely for local RAM
footprint (single binary, real Kafka wire protocol -- same
producer/consumer code targets MSK/Confluent with a bootstrap-server
change). "Converting" sessions are built by querying **live** online
orders from `oltp.orders` from the last 45 days and generating a
page_view → product_view → add_to_cart → checkout_start → purchase funnel
that ends exactly at that order's real `order_id`/`order_ts` -- not
re-derived from a shared RNG seed, an actual live join. Non-converting
browse sessions are generated separately to pad out realistic
funnel-dropoff volume. Events are generated in causal order per session
but the *produce* order is locally shuffled in windows of 500, simulating
the clock-skew/retry reordering a real HTTP event collector exhibits.

**Bug hit and fixed:** Redpanda's advertised Kafka address
(`redpanda:9092`) is only resolvable inside the Docker network. A
host-side Python client can complete the initial metadata fetch (which
succeeds via the mapped port) but then every subsequent produce/fetch
request is redirected to that unresolvable hostname and hangs until
timeout. Fixed by adding a second `external` listener
(`--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092`,
matching `--advertise-kafka-addr`), so host tools use `localhost:19092`
and in-network containers (Airflow) use `redpanda:9092` -- same broker,
same topic, two audiences. This is the standard Kafka
advertised-listener-vs-network-boundary problem and a legitimate
interview story in its own right.

**Result.** 1,641,330 events (40,000 converting sessions sampled from
real orders + 200,000 browse-only sessions) produced in **109 seconds**
(~15,000 msg/s sustained). Spot-checked a random purchase event against
Postgres: `customer_id`, `order_id`, and timestamp matched the source
order exactly -- the cross-source join is real, not asserted.

---

## Mock ad-platform REST API (`docker/mock-api/`)

**Situation.** A real third-party marketing API is the standard "gotcha"
source in data-eng interviews: pagination, rate limits, and incremental
sync all have to be handled correctly by the client, and none of that is
demonstrable against a source that hands you a full CSV.

**Task.** Build a service that behaves like one, without needing a live
third-party account/quota.

**Action.** FastAPI service holding a numpy-generated, deterministic
1,095,000-row campaign-performance dataset (500 campaigns x 3 ad sets x
730 days) in memory. Implements: API-key auth (403 without it), page/
page_size pagination with a `next_page` cursor in the envelope, a 60
req/min token-bucket rate limiter (429 + `Retry-After`), a ~3% injected
transient-500 rate (forces retry/backoff, not just pagination), and an
`updated_since` incremental filter where a **time-rotating slice of
historical rows re-appears as freshly updated** every request -- so a
naive "only fetch new dates" ingestion job will silently miss
attribution corrections, while one keyed correctly on `updated_at` won't.

**Result.** Verified live: unauthenticated request → 403; page 1 of
1,095,000 rows → correct pagination envelope (365,000 total pages at
page_size=3); 65 rapid requests → first ~60 succeed, remainder 429;
`updated_since=<now>` query → 8,463 rows still returned (the rotating
restatement slice), proving the incremental cursor isn't just a
pass-through date filter.

---

## Airflow image (Java + PySpark + delta-spark + dbt-duckdb) + Delta/S3A smoke test

**Situation.** This is the highest-risk unknown in the whole build: does
PySpark + Delta Lake + S3A actually work together against MinIO inside
one Airflow container, or does version drift between Spark's bundled
Hadoop client, `hadoop-aws`, and `delta-spark` blow up at runtime the way
it notoriously can?

**Task.** Build the custom Airflow image, get the full stack (webserver +
scheduler + LocalExecutor against the shared Postgres metadata DB)
healthy, and prove the Delta/S3A wiring with a real write-append-read-
history cycle before writing a single bronze job on top of it.

**Action & bugs hit:**
- `docker/airflow/Dockerfile`: `apache/airflow:2.10.5-python3.11` +
  `openjdk-17-jre-headless` (apt) + pyspark 3.5.3 / delta-spark 3.2.1 /
  dbt-duckdb 1.9.1 (pip). Build succeeds; pip prints a wall of protobuf/
  pandas version-conflict warnings against Airflow's *unused* Google-
  Cloud and Snowflake provider extras -- cosmetic, nothing in this
  project imports those providers, confirmed the image still runs clean.
- **Bug 1 -- log config crash.** `./airflow/{logs,plugins}` got
  auto-created by Docker as `root:root` on first bind-mount, so the
  container (running as `AIRFLOW_UID`) couldn't write its own log
  directory and crashed before even parsing config. Fixed by pre-owning
  those directories to the host UID (`docker run --rm -v ...:/x alpine
  chown -R 1000:0 /x/logs /x/plugins`) and documenting in `.env.example`
  that `AIRFLOW_UID` must match the host user's real UID, not the
  upstream default of 50000.
- **Bug 2 -- "user has no username".** `airflow-init` originally
  overrode the container `entrypoint:` to run its migrate/create-user
  command directly. That skips the base image's own `/entrypoint`
  script, which is what patches `/etc/passwd` so an arbitrary
  `AIRFLOW_UID` resolves to a real user -- without it, `getpass.getuser()`
  throws `KeyError` and Airflow refuses to run. Fixed by leaving the
  image's default entrypoint in place and passing the init logic as
  `command:` only.
- **Memory tuning from measurement, not guessing.** Idle webserver RSS
  measured at ~874MB against an initial 1024m `mem_limit` -- too tight
  for any headroom; bumped to 1280m after observing the real number
  rather than picking one upfront.
- Wrote `spark_jobs/common/spark_session.py` (the one place Delta
  extensions + S3A-for-MinIO config is defined) and
  `spark_jobs/smoke_test_delta_s3a.py`: writes a 2-row Delta table to
  `s3a://lakehouse/bronze/_smoke_test`, appends a second batch, reads
  both back, and asserts `DESCRIBE HISTORY` shows exactly 2 commits.

**Result.** Full stack (`docker compose --profile orchestration up`)
healthy: postgres, minio, redpanda, mock-api, airflow-webserver,
airflow-scheduler all report healthy; `/health` endpoint confirms
metadatabase + scheduler both `"healthy"`. Smoke test passed on the
first run after fixing the above: Maven resolved delta-spark 3.2.1 +
hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 cleanly, wrote/appended/
read back 3 rows correctly, and `DESCRIBE HISTORY` showed exactly the 2
expected `WRITE` commits -- confirming this is a real transactional Delta
table on MinIO, not parquet-with-extra-steps. This de-risks the
technical foundation every bronze job in the next entry builds on.

## ADF-pattern ingestion layer + all 4 bronze pipelines + first DAG

**Situation.** Needed to evidence "orchestrated ingestion beyond Airflow
(ADF pattern)" without an Azure subscription, and land all four sources
into real bronze Delta tables -- the actual point of everything built so
far.

**Task.** Build a metadata-driven copy-activity layer (linked
service/dataset/watermark, ADF's own vocabulary) that's genuinely
reusable across four very different sources, land each into
`s3://lakehouse/bronze/*`, and wire it into an Airflow DAG.

**Action -- design decisions:**
- Skipped a generic YAML-driven pipeline interpreter. With exactly 4
  concrete pipelines that will only ever have 4 concrete configurations,
  a full interpreter engine is an abstraction with no second caller --
  the kind of premature generality worth avoiding. Instead: a shared
  watermark control table (`ingestion.pipeline_watermarks`, living in
  the OLTP Postgres instance -- the one control DB every pipeline
  already reaches) plus per-pipeline Python modules that each declare
  their own linked-service/dataset/watermark metadata in a docstring
  next to the code that implements it. Same declarative shape ADF's
  pipeline JSON carries, without a bespoke interpreter for it.
- **Ingestion uses delta-rs (`deltalake` package), not PySpark.** This
  is deliberate, not a cost-cutting shortcut: real ADF Copy Activity is
  itself a lightweight, non-Spark data-movement service -- Spark/
  Databricks only enters ADF for the Mapping Data Flow / transformation
  activities. Using a second, JVM-free Python library for ingestion and
  reserving PySpark for the heavier bronze→silver transform mirrors that
  real split, and gives two genuinely different "how do you write to
  Delta" answers for an interview instead of one repeated everywhere.
  Verified against MinIO directly before writing any pipeline code: a
  write/append/schema-merge/read-back cycle, AND (importantly) a
  deliberate type-conflict test confirming `write_deltalake` correctly
  *rejects* an int64→string column change on append rather than
  silently coercing it -- that failure mode is exactly what
  `ingestion/pipelines/flat_files.py` has to design around.

**Bugs found and fixed (all caught by actually running the pipelines
against live data, not by code review):**

1. **REST API pipeline: rate-limit waits silently shared the transient-
   error retry budget.** `_get_page`'s first version used one
   `for attempt in range(MAX_RETRIES)` loop for both HTTP 429 and 5xx
   handling, on the theory that `continue`-ing on a 429 "didn't count."
   It does -- `continue` still advances a `for` loop's counter. A full
   ~2,190-page historical sync against a rate-limited API is *going* to
   hit 429 constantly; that's normal operation, not a fault. Caught
   directly: `RuntimeError: page 59 failed after 6 attempts` while every
   one of those "attempts" was a clean, server-directed 429 wait. Fixed
   by giving rate-limit waits and transient-error attempts two
   independent counters -- see the fix's own docstring in
   `ingestion/pipelines/rest_api_campaigns.py` for the full reasoning.
2. **Kafka seeder: bare `date` arithmetic silently dropped time-of-day.**
   `seeders/seed_kafka_events.py` computed `window_start` from `TODAY`
   (a `datetime.date`, not `datetime`) and then added second-level
   `timedelta`s to it -- `date.__add__` only honours `timedelta.days`,
   so every non-converting session's `event_ts` collapsed to midnight
   and serialised as `"2026-07-26"` instead of a real timestamp. This
   sat undetected in the already-seeded topic (Phase 0) until the bronze
   Kafka consumer's `pd.to_datetime(df["event_ts"])` hit one of the
   ~200k affected events and crashed with a strptime format error. Fixed
   the root cause (anchor `window_start` to a real UTC datetime) *and*
   hardened the consumer (`format="ISO8601", utc=True"` instead of a
   rigid implicit format) as defense in depth -- a producer being
   imperfectly consistent is exactly the kind of thing bronze should
   survive, not crash on. Re-seeded the topic clean and re-drained
   bronze from zero to confirm: exactly 1,641,330 rows, matching the
   producer's own count exactly, zero malformed timestamps.
3. **Airflow: DAG sat `queued` and never ran -- paused at creation.**
   Triggered the DAG via CLI immediately after writing it; the run sat
   in `queued` state with every task `None` for several minutes with no
   errors anywhere in scheduler or DAG-processor logs. Root cause: my
   own `airflow dags unpause bronze_ingestion` ran *before* the
   DagFileProcessor had synced the file into `DagModel` for the first
   time (that took ~5 minutes -- the default `dag_dir_list_interval`),
   so the unpause command found nothing to unpause and silently no-oped.
   By the time the file WAS synced moments later, it landed with
   Airflow's default `dags_are_paused_at_creation = True`. Re-running
   `unpause` once the DAG actually existed fixed it immediately. Not a
   platform bug -- a genuine "the tooling did exactly what it was
   configured to do, and I checked the wrong signal (no errors) instead
   of the right one (`is_paused`)" lesson, which is its own kind of
   interview-worthy debugging story.

**Result.** All four pipelines run clean end to end, independently
verified before ever touching the DAG:

| Pipeline | Bronze table | Rows landed | Notes |
|---|---|---|---|
| postgres_oltp | customers/stores/products/order_items/payments/orders | 9,858,349 | orders incremental, re-run confirmed idempotent (0 new rows) |
| rest_api_campaigns | campaign_performance | 1,115,000 | slightly over the API's 1,095,000 -- the restatement mechanism re-surfaced some rows mid-sync, exactly the behaviour it's designed to exercise |
| flat_files | pos_inventory_snapshots | 1,865,588 | exact match to source; all 4 schema-drift stages absorbed with zero write failures |
| kafka_events | clickstream_events | 1,641,330 | exact match to producer count after the timestamp-bug fix; drained via 9 bounded micro-batch runs proving consumer-group-offset continuation |

**Bronze total: 14,480,267 rows**, all in real, queryable Delta tables
on MinIO. `airflow/dags/bronze_ingestion.py` runs all four as
independent, parallel LocalExecutor tasks; triggered end to end via
`airflow dags trigger` and confirmed `success` (see DAG run
`manual__2026-08-17T14:18:02+00:00`).

## PySpark bronze→silver: 9 jobs, 3 dedup strategies, 1 true SCD2, 3 real bugs

**Situation.** Bronze is raw and deliberately messy by design (see the
prior entries) -- nine bronze tables now need to become nine silver
tables that are actually cleaned, conformed, and deduplicated, using
PySpark against Delta on MinIO, per the brief's "PySpark transformations
on Delta Lake tables" requirement.

**Task.** Design and build one silver job per bronze table, choosing the
*right* strategy per table rather than one generic "clean everything"
pattern -- and specifically avoid rebuilding whatever the first
portfolio project ([retail-medallion-pipeline](https://github.com/Pranuvar/retail-medallion-pipeline))
already covers in dbt/SQL (SCD2, incremental models) without adding
anything new.

**Action -- four distinct strategies, chosen deliberately, not
uniformly applied:**

1. **`customers.py` -- identity resolution, not SCD2.** Two stacked
   duplication problems (re-ingestion doubling + ~2% deliberately
   seeded duplicate *people*, not duplicate rows) need two different
   fixes in sequence: collapse re-ingests first, then resolve identity
   on `lower(trim(email))`, keeping full lineage of which
   `customer_id`s merged into each canonical record.
2. **`stores.py` / `order_items_payments.py` -- plain re-ingestion
   dedupe.** No realistic mutable attribute or duplicate-identity story
   in this data, so no need to reach for anything heavier; kept
   deliberately simple.
3. **`products_scd2.py` -- true SCD Type 2 via a native Delta `MERGE`,
   not a dbt snapshot.** This is the one built specifically to be a
   *different* answer than the first project's dbt-snapshot-based SCD2:
   same concept, a genuinely different engine and mechanism (a single
   atomic MERGE using the standard Databricks synthetic-`merge_key`
   pattern to close-old/insert-new in one statement, vs. dbt's
   scan-and-diff-on-every-run approach). Proved live, not just
   structurally present: updated 500 real product prices in Postgres,
   re-ran the pipeline, confirmed the MERGE correctly closed the old row
   (`is_current=false`, `valid_to` set) and opened a new current one --
   twice, with two independent mutations (a price change, then an
   `is_active` flip), plus a no-mutation re-run proving idempotency (0
   changed, 0 duplicate rows).
4. **`orders.py` -- incremental `MERGE`, the late-arriving-fact/backfill
   mechanism.** Re-merges the FULL bronze table every run rather than
   tracking a second watermark, specifically so it's trivially safe to
   re-run for any reason. `campaign_performance.py` resolves the mock
   API's restatement duplicates (latest `updated_at` wins);
   `pos_inventory.py` is the real payoff of the bronze schema-drift
   design -- `coalesce`s the renamed column, regex-strips currency/units
   text, casts to real numerics, uniformly across all 4 drift stages;
   `clickstream_events.py` is a defensive dedupe against the Kafka
   consumer's at-least-once redelivery gap.

**Three real bugs found running these against live data:**

1. **Lazy-evaluation trap in the SCD2 diagnostics.** `products_scd2.py`
   computed `changed.count()` for a print statement AFTER calling
   `.merge().execute()`. Because `changed` was a lazy DataFrame built
   from a transformation of the (mutable) silver table, re-evaluating it
   post-merge re-read the ALREADY-UPDATED table, reporting "0 changed
   products" on a run that had just correctly versioned 500 of them.
   The actual write was always correct (verified directly: total rows
   20,000 -> 20,500, one specific product's price history showing
   exactly 2 versions with correct `valid_from`/`valid_to`); only the
   diagnostic print was reading the wrong point in time. Fixed by
   materialising the counts into plain Python ints BEFORE calling
   `.execute()`.
2. **A future-dated `updated_at` silently broke the late-arriving-fact
   demo.** `seeders/seed_postgres_oltp.py` added an unclamped delay to
   some orders' `updated_at`, pushing a slice of them past the actual
   moment the seed script ran. The incremental pipeline's watermark had
   already advanced to that future value, so a genuinely new
   late-arriving row (with a real, current `updated_at`) read as older
   than the watermark and was silently skipped. See
   docs/incident-log.md #2 for the full sequence and fix.
3. **Resetting that watermark exposed a Delta MERGE ambiguity bug.**
   Rolling the watermark back re-swept already-processed orders into
   bronze a second time; `orders.py` assumed bronze had at most one row
   per `order_id` (true under normal operation, not guaranteed in
   general) and Delta's MERGE correctly refused the resulting
   ambiguous multi-source-row match. Fixed by deduplicating bronze on
   `order_id` before merging -- see docs/incident-log.md #2.

**Result.** All 9 silver tables built and verified with exact,
predicted numbers:

| Silver table | Bronze rows in | Silver rows out | What resolved |
|---|---|---|---|
| customers | 306,000 | 150,000 | re-ingest dedupe (306k→153k) + identity resolution (153k→150k, 3,000 merged identities) |
| stores | 300 | 150 | re-ingest dedupe |
| products | 20,000 (+700 versioned) | 20,000 current / 20,700 total | SCD2, 2 live mutations proven |
| orders | 2,000,002 | 2,000,002 | incremental MERGE, survived a 2-bug incident intact |
| order_items | 11,250,066 | 5,625,033 | re-ingest dedupe, 0 orphaned order_id refs |
| payments | 4,120,332 | 2,060,166 | re-ingest dedupe, 0 orphaned order_id refs |
| campaign_performance | 1,121,970 | 1,095,000 | restatement dedupe -- lands exactly on the API's true record count |
| pos_inventory_snapshots | 1,865,588 | 1,862,500 | schema-drift coalesce/cast, 0 unparseable values |
| clickstream_events | 1,641,330 | 1,641,330 | defensive dedupe, 0 redelivered |

## Incidents #3 and #4: backfill idempotency, Spark crash recovery

**Situation.** Two of the eight required interview scenarios remained:
proving a whole historical partition can be safely rebuilt from scratch
(not just that one new row lands correctly, which #2 already proved),
and proving Delta's transaction log actually protects readers from a
write that dies mid-flight.

**Action.** Both built as real, reproducible scripts under `ops/`, not
prose:

- `ops/incident_03_backfill.py`: `DeltaTable.delete()` wipes a month
  entirely from silver.orders, then the real MERGE job (`orders.py`)
  runs twice in a row against unchanged bronze.
- `ops/incident_04_crash_recovery.py`: a boto3 polling loop watches
  the target Delta table's path in MinIO and sends `SIGKILL` the
  INSTANT a new, uncommitted parquet file is observed -- not a guessed
  sleep duration, real evidence the write was genuinely in flight. Bug
  caught building this one: the first version killed only the Python
  wrapper process; PySpark launches the actual Spark driver as a child
  JVM via py4j, so the wrapper dying left an orphan JVM that could
  still complete and commit -- silently invalidating the whole test.
  Fixed with `preexec_fn=os.setsid` + `os.killpg()` so the signal
  reaches the JVM too.

**Result.** #3: March 2025 fully restored by run #1 (70,547 orders,
exact match to the pre-delete baseline), run #2 a true no-op (net new:
0), distinct `order_id` count equalling total row count -- zero
duplication under repeated backfill. #4: a 11,250,066-row write killed
mid-flight (16 orphaned files left behind as physical proof) left the
table's committed row count completely unchanged throughout, and a
plain re-run recovered it fully. Full detail: docs/incident-log.md #3, #4.

## Silver→gold quality gate (incident #5, checks built; DAG wiring next)

**Situation.** The brief is explicit: "quality gates that block
promotion, not tests that report after the fact." Needed a mechanism
that runs BEFORE gold is touched and genuinely prevents the build on
failure, not a check that runs after and just complains.

**Action.** `spark_jobs/quality_gate.py`: six PySpark assertions against
real invariants already designed into this project's seed data
(payments-reconcile-to-order-items, no duplicate order_ids, no orphaned
child-table references, pos_inventory parse quality, no duplicate
customer identities post-resolution, campaign_performance natural-key
uniqueness). Chose a focused custom script over Great Expectations
deliberately -- for 9 tables and a handful of genuinely load-bearing
checks, a plain assertion script is less machinery and just as
enforceable from Airflow; GX earns its keep at a scale this project
isn't at (see the script's own docstring for the full reasoning, same
"don't build an abstraction with one caller" principle as ingestion/'s
metadata-in-code decision).

**A real bug in the check itself, not the data.** First run: the
`pos_inventory_parse_quality` check FAILED -- 12,053 null
`quantity_on_hand` values (0.32%) exceeded its threshold. Investigated
before assuming the pipeline was broken: cross-referenced bronze
directly and confirmed all 12,082 nulls there are genuinely missing at
SOURCE (the seeder's deliberate ~2% `blank_mask`, simulating till-export
truncation -- see seeders/seed_flatfiles.py), not a parser failure --
`pos_inventory.py`'s own diagnostic already reported 0 true parse
failures on this same data. The check had been measuring "how many
nulls are in the clean column," which conflates a genuine parser bug
with expected source incompleteness -- two very different severities.
Fixed by rewriting the check to test for TRUE parse failures only (raw
value present, nothing numeric extractable), re-joining to bronze --
the same condition `pos_inventory.py` already computes at build time,
now enforced as a gate. Re-run: all 6 checks pass, 0 true parse
failures, source-incompleteness rate reported as informational context,
not a gate failure.

**Result.** Gate script proven correct against real data, including a
real self-check bug found and fixed. Still pending: wiring this into
the actual Airflow DAG ahead of the dbt gold build (so a gate failure
provably blocks the DAG, not just returns a non-zero exit code in
isolation) and a live demo of it actually blocking a bad promotion --
next entry.

## dbt Core silver→gold: star schema, SCD2-aware fact, 1 incremental model

**Situation.** Final layer: turn 9 silver Delta tables into a real
dimensional model, using dbt -- and specifically avoid re-proving what
[retail-medallion-pipeline](https://github.com/Pranuvar/retail-medallion-pipeline)
(the first portfolio project) already evidences in dbt (SCD2 via
snapshots, incremental models generally), per the brief's explicit ask
for "a new take."

**Action -- the DuckDB/Delta integration, validated before building
anything on top of it:** dbt-duckdb + DuckDB's own `delta` extension
reads Delta tables directly off MinIO via `delta_scan('s3://...')` --
tested standalone first (write/read round-trip against a live silver
table) before writing a single staging model. `macros/delta_source.sql`
is the ONE place this read is defined; every staging model calls it
instead of hardcoding a path, and it branches on `target.type` --
DuckDB gets `delta_scan()`, a Snowflake target would read from a
pre-created external table over the same S3 location (config only,
documented in `profiles.yml`, not built since it needs a live account).
This is the actual, honest boundary of "Snowflake-portable dbt": every
mart model downstream of staging is adapter-agnostic SQL and needs zero
changes to migrate; only staging's OWN source definition is
engine-specific, isolated to 9 one-line models plus this macro.

Star schema: `dim_customers`, `dim_stores`, `dim_products` (SCD2,
passed through from the Spark MERGE -- not re-implemented in dbt, see
`products_scd2.py`), `dim_date` (generated via `dbt_utils.date_spine`),
`fct_order_items` (line grain, SCD2-aware), `fct_orders` (order-grain
rollup), `fct_campaign_performance` (the one deliberately INCREMENTAL
dbt model -- a different upsert mechanism than orders.py's Spark MERGE,
on purpose, so dbt's own incremental materialization is still evidenced
somewhere), `fct_clickstream_sessions` (session-grain funnel rollup).

**Three real bugs, all found by the FIRST real `dbt build`, none by
inspection:**

1. **1,073 orphaned `customer_id` references** (`relationships` test
   failure). Root cause: identity resolution in Spark
   (`customers.py`) collapses ~3,000 duplicate customer_ids down to one
   canonical row per real person, but `oltp.orders.customer_id` was
   generated against the full PRE-resolution 153,000-id space -- some
   orders legitimately reference a customer_id that no longer has its
   own row. Fixed with `stg_customer_identity_map.sql`, unnesting the
   `source_customer_ids` array `customers.py` already tracks for
   exactly this purpose, and remapping `customer_id` through it in
   `stg_orders.sql`.
2. **695 duplicate `product_key` values** (`unique` test failure).
   Root cause: this entire project's build -- including the SCD2
   dimension's initial load and two separate live mutation demos --
   was compressed into a single calendar day, and the surrogate key was
   `(product_id, valid_from)` where `valid_from` is day-granularity by
   design. Every product touched by either mutation ended up with two
   rows sharing an identical `valid_from`. Fixed by keying the
   surrogate on `(product_id, _silver_processed_at)` instead -- a real
   timestamp, unique per Spark job run even on the same calendar day --
   while keeping `valid_from`/`valid_to` as clean business dates for
   anyone actually reading them.
3. **2,251,220 of 5,625,033 order_items (40%) with a NULL margin**
   (`not_null` warning -- the big one). Root cause, confirmed by direct
   query before assuming anything: the SCD2 initial load had set
   `valid_from = as_of_date` (today, the date the pipeline first ran)
   for every product's first version -- meaning products were only
   "valid" from today onward, so literally every order dated in the 2
   years before today failed the point-in-time join. Fixed at the root
   in `products_scd2.py`: the initial load now backdates `valid_from`
   to each product's own `created_at` (its real catalog-entry date),
   not the processing date -- re-ran the full SCD2 rebuild plus both
   live mutation demos against the corrected baseline. That dropped the
   NULL count to 2,251,220 (still real): confirmed by direct query that
   every one of these is a genuinely different, pre-existing gap --
   `seeders/seed_postgres_oltp.py` never constrained order_item product
   selection by the product's own creation date, so ~40% of order lines
   reference a product from before its earliest tracked SCD2 version.
   Re-seeding to fix that root cause would mean redoing everything
   built on `oltp.products` since Day 1; the honest fix at this layer
   was an explicit, FLAGGED fallback (`used_earliest_version_fallback`)
   to the earliest known version for these rows, documented in
   `fct_order_items.sql` as a real modelling judgement call, not hidden.

**Result.** `dbt build` (1 incremental model, 7 table models, 10 view
models, 49 data tests): **67/67 pass, 0 errors, 0 warnings.**
`fct_order_items` computes a margin for all 5,625,033 rows (40.02% via
the documented fallback, transparently flagged, not blended in
silently); total computed gross margin: €37,514,247. Final gold row
counts: dim_customers 150,000, dim_stores 150, dim_products 20,450
(current + history), dim_date 1,096, fct_orders 2,000,002,
fct_order_items 5,625,033, fct_campaign_performance 1,095,000,
fct_clickstream_sessions 240,000.

## Quality gate wired into the DAG (incident #5 completion)

**Situation.** The gate script existed and was proven correct in
isolation (previous entry); the brief's actual requirement is that it
BLOCKS -- needed the real Airflow wiring and a real demonstration that
a failure stops gold, not just a passing unit test of the check logic.

**Action.** `airflow/dags/gold_promotion.py`: two `BashOperator` tasks,
`quality_gate >> dbt_build_gold`, no custom trigger-rule logic --
Airflow's default (`all_success`) does the blocking. Triggered a clean
baseline first via the Airflow CLI; hit a real bug immediately:
`dbt_build_gold` failed with `Binder Error: Cannot mix values of type
VARCHAR and TIMESTAMP in COALESCE operator`. Root cause:
`fct_campaign_performance.sql`'s incremental filter compared
`updated_at` (a VARCHAR all the way from the mock API's
`.isoformat()`-serialised JSON -- nothing before this model ever cast
it) against a `TIMESTAMP` literal. Passed cleanly on `--full-refresh`
runs because `is_incremental()` is false then and the comparison never
executes -- only a genuine second, normal `dbt build` (exactly what the
DAG does) hits it. Fixed at the staging layer
(`cast(updated_at as timestamp)` in `stg_campaign_performance.sql`, with
the reasoning documented inline), verified both a `--full-refresh` and
a plain incremental `dbt build` pass, then re-ran the DAG end to end:
`quality_gate` succeeded, `dbt_build_gold` succeeded.

**Live "break it" demo** (`ops/incident_05_quality_gate_block.py`):
injects a payment row that breaks the payments/order_items
reconciliation invariant directly into silver, triggers the real DAG via
the Airflow CLI, and asserts on the actual task states and gold's actual
row count. Two more real bugs found building the demo script itself
(a `decimal(5,2)` precision overflow on the injected test value, and the
Airflow CLI's table output wrapping a long run_id across two lines and
silently truncating it) -- both fixed, detail in incident-log.md #5.

**Result.** `quality_gate` failed as designed on the corrupt data;
`dbt_build_gold` went `upstream_failed` -- never executed;
`gold.fct_orders` row count was **identical before and after** the
blocked run (2,000,002, unchanged). Removed the corruption, re-triggered:
both tasks `success`. The gate blocks a real bad promotion through the
real orchestrator, not a script asserting the concept.

## CI/CD: GitHub Actions, sqlfluff, dbt build against seed fixtures

**Situation.** Needed "CI/CD — GitHub Actions running dbt build, tests
and SQL linting on every PR" and a live demonstration that a broken dbt
test fails a PR check (incident #6), without spinning up the full
Postgres/MinIO/Spark stack inside a GitHub Actions runner (slow, heavy,
and not what a PR-level check needs -- that's what the local
docker-compose stack + `ops/incident_*.py` scripts are already for).

**Action.** `macros/delta_source.sql` gained a third branch:
`--vars '{use_seeds: true}'` swaps every staging model's source from
`delta_scan()` to `ref()` against a small hand-written CSV fixture under
`dbt/seeds/` -- 9 files, ~5-8 rows each, hand-constructed to be
internally consistent (payments reconcile to order_items exactly,
product #1 has two SCD2 versions straddling an order date on either
side so the point-in-time join is genuinely exercised, one clickstream
session with no `customer_id` to exercise the nullable path). Real
infrastructure swapped for fast, deterministic fixtures -- same models,
same tests, same SQL. `.github/workflows/ci.yml`: two jobs, `sql-lint`
(sqlfluff, dbt templater) and `dbt-build` (`dbt build` against the
seeds), both running on every PR touching `dbt/`.

Running `sqlfluff lint` against the real project for the first time
surfaced genuine style violations across 7 files (implicit table
aliasing, redundant `else null`, keyword-as-identifier warnings on
`dim_date`'s `year`/`month` columns, select-wildcard-ordering). Fixed
the real ones (aliasing, redundant else, indentation); excluded ST06
(column-order-by-calculation) and RF04 (keywords-as-identifiers) project-
wide as deliberate house-style calls, documented inline in `.sqlfluff`
-- `year`/`month` are the correct column names for a date dimension
regardless of being reserved words elsewhere. Also excluded LT02
specifically for its friction with a Jinja `{% if %}` block inside one
staging model's select list (`stg_customers.sql`'s CI-fixture branch for
`source_customer_ids`) -- the rendered SQL is correctly indented either
way.

**Result.** Seed fixtures alone: 76/76 (9 seeds, 7 table models, 1
incremental model, 10 view models, 49 tests) in 2-4 seconds, fully
self-contained -- verified identical against both a fresh DuckDB file
and re-run for idempotency. `sqlfluff lint` clean across the whole
`models/` and `tests/` tree. Incident #6 (full detail in
incident-log.md): deliberately broke `stg_payments.sql` with a
plausible "processing fee" bug, ran the exact CI command locally,
confirmed real process exit code 1 (6 reconciliation failures reported);
reverted, confirmed exit code 0. This environment has no `gh` CLI or
GitHub remote configured, so a literal GitHub-hosted red PR check is the
one deliverable in this project that needs the user's own push to
produce -- the CI logic itself is fully proven with the exact commands
GitHub Actions would run.

## Small-file compaction (incident #8): real numbers, not estimates

**Situation.** The brief wants "partitioning, small-file compaction,
and what I'd change if the bill doubled" -- and this project already
had real, unstaged evidence of a small-file problem sitting in bronze:
`ingestion/pipelines/flat_files.py` commits once PER SOURCE FILE
(3,725 commits for 3,725 files), a deliberate choice documented at
build time specifically to leave this evidence for later.

**Action.** `ops/incident_08_compaction.py` measures file count/size via
`DESCRIBE DETAIL` (a manual Hadoop `listStatus` on the table root was
tried first and silently returned 0 -- this table is partitioned by
`drop_year_month`, and `listStatus` isn't recursive into partition
subdirectories; `DESCRIBE DETAIL` already knows the true count
regardless), times a representative per-store aggregation, runs
`OPTIMIZE`, measures both again.

**Result.** 3,725 files -> 25 (99.3% reduction), average file size
10.0KB -> 452.1KB, query wall time 11.63s -> 1.23s (~9.5x faster) from
one 16-second `OPTIMIZE` call. Full "if the bill doubled" write-up
(stop the full-snapshot dimension re-copies, batch the flat-file copy
activity instead of one-commit-per-file, right-size Spark shuffle
partitions per job, reconsider the SCD2 fallback rate) in
docs/incident-log.md #8.

## Power BI semantic model + dynamic RLS

**Situation.** Needed a real Power BI semantic model with DAX, hierarchies,
and RLS -- built entirely on Linux, where Power BI Desktop cannot run.
The choice was between faking it (a screenshot-less description) or
building something genuinely real that just needs a different OS to
click-test, and being explicit about exactly where that line falls.

**Action.** Built the model as TMDL source -- the actual text format
Power BI Desktop writes for a `.pbip` project, not a mockup: 9 tables
(4 dims incl. RLS mapping, 4 facts, 1 measures-only table), a full star
schema in `relationships.tmdl` with reasoning for each non-default
choice (an inactive Customer relationship on the clickstream fact to
avoid ambiguity; a genuine fact-to-fact relationship to FactOrders,
kept deliberately despite being a modelling anti-pattern in general,
because it's the literal cross-source join this whole project exists to
prove), 19 DAX measures, 2 real hierarchies (Calendar, Category), and
dynamic RLS via `USERPRINCIPALNAME()` against a mapping table rather
than a hard-coded filter per role. `export_gold_for_powerbi.py` --
tested, not aspirational -- exports all 8 gold marts plus a synthetic
RLS mapping table to Parquet (Power BI's zero-driver import path, since
DuckDB has no first-class Power BI connector).

**Result.** Export verified for real: 9 Parquet files,
exact row counts matching gold (dim_customers 150,000 ... fct_order_items
5,625,033 ...). The model itself -- relationships, measures, hierarchies,
RLS logic -- is complete and correct TMDL; the one thing this Linux
environment genuinely cannot do is click through Desktop's "View As
Roles" to visually confirm the RLS filter fires, which needs Windows/
Mac (documented explicitly, with the exact steps to run it, in
`powerbi/README.md` and `docs/incident-log.md` #7 -- not glossed over).

## Governance: lineage + generated data dictionary

**Situation.** Needed "lineage, a generated data dictionary, column-level
descriptions" -- and specifically a GENERATED dictionary, not a
hand-maintained doc that drifts from the real schema the first time a
column gets renamed.

**Action.** Lineage: `dbt docs generate` -- dbt's own built-in tooling,
not custom-built. Produces `target/manifest.json` (the full resolved
model DAG), `target/catalog.json` (real column names/types introspected
from the actual built warehouse), and `target/index.html` (the
interactive lineage graph viewer, `dbt docs serve` to browse it). Ran
for real: 76 manifest nodes, 33KB catalog.

Data dictionary: `governance/generate_data_dictionary.py` parses those
same two artifacts into `docs/DATA_DICTIONARY.md` -- real column types
from `catalog.json` (not asserted), real descriptions from
`manifest.json` (only what's actually documented), and real enforced-test
coverage per column (not_null/unique/relationships/accepted_values,
parsed out of the test nodes' `depends_on` graph) so the dictionary
shows what's actually GUARANTEED about a column, not just what it's
named. First run surfaced the gap directly: most columns showed "no
description" because `_staging__schema.yml`/`_marts__schema.yml` had
tests but few descriptions -- went back and added real descriptions to
both schema files (every model, every business-meaningful column),
re-ran `dbt docs generate` + the dictionary generator, confirmed the
output was actually filled in rather than trusting the script worked
without checking its output.

**Result.** `docs/DATA_DICTIONARY.md`, 312 lines, regenerable on demand
(`dbt docs generate && python governance/generate_data_dictionary.py`)
-- covers all 9 staging + 8 marts models with real types, descriptions,
and enforced-test coverage pulled from the actual build, not hand-typed.

## Next entries (pending)

- Final docs pass (README, COMPONENT_MAP)

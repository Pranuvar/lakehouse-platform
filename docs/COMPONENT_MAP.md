# Component Map

Every file/directory mapped to the job-spec requirement(s) it evidences.
Purpose: when a job spec lists "PySpark", "ADF", "Power BI RLS", etc.,
this is the file to open to back that line up. Status: ✅ built & verified
· 🟡 built, one specific piece needs an environment this build doesn't
have (called out explicitly) · 🔲 planned, not yet built.

| Requirement (as it appears in job specs) | Evidenced by | Status |
|---|---|---|
| **PySpark on Delta Lake** | `spark_jobs/bronze_to_silver/` -- 9 jobs (identity resolution, SCD2 via native `MERGE`, incremental upsert, schema-drift coercion, dedup) | ✅ |
| **Orchestration beyond Airflow (ADF pattern)** | `ingestion/` -- watermark control table + 4 copy-activity pipelines, metadata-in-code (not a generic YAML interpreter -- see BUILD_LOG for why) | ✅ |
| **Airflow orchestration** | `airflow/dags/bronze_ingestion.py`, `airflow/dags/gold_promotion.py` -- both triggered and confirmed `success` end to end through the real scheduler | ✅ |
| **PySpark + Delta Lake + S3A wiring** | `spark_jobs/common/spark_session.py` -- verified: write/append/read, transaction log, mid-write crash recovery (incident #4) | ✅ |
| **Power BI semantic model, DAX, hierarchies** | `powerbi/model/` -- TMDL source, 9 tables, 19 DAX measures, 2 hierarchies (Calendar, Category) | ✅ |
| **Row-level security (Power BI)** | `powerbi/model/roles.tmdl` -- dynamic RLS via `USERPRINCIPALNAME()` + lookup table | 🟡 model + DAX real and complete; the Desktop "View As Roles" click-through needs Windows/Mac (this build is Linux) -- see incident-log.md #7 |
| **CI/CD (GitHub Actions, dbt build/test, SQL lint)** | `.github/workflows/ci.yml` -- sqlfluff + `dbt build` against seed fixtures, both jobs verified locally with the exact commands CI runs (real exit codes 0/1) | 🟡 CI logic fully proven locally; a literal GitHub-hosted PR check needs this repo pushed to GitHub (no `gh`/remote configured in this build env) -- see incident-log.md #6 |
| **dbt Core (silver→gold, star schema, SCD2, incremental)** | `dbt/` -- 4 dims + 4 facts + 1 measures table, 49 tests, 1 incremental model, SCD2 in Spark (not dbt, deliberately -- see BUILD_LOG) | ✅ |
| **Governance: lineage, data dictionary, column descriptions** | `dbt docs generate` (real lineage graph) + `governance/generate_data_dictionary.py` -> `docs/DATA_DICTIONARY.md` (generated, not hand-maintained) | ✅ |
| **Streaming / near-real-time ingestion** | `ingestion/pipelines/kafka_events.py` -- bounded micro-batch consumer, consumer-group offsets as watermark; verified exact 1,641,330-row drain, zero loss/duplication | ✅ |
| **Postgres OLTP source, incremental extraction** | `ingestion/pipelines/postgres_oltp.py` -- watermark-incremental `orders`, re-run confirmed idempotent | ✅ |
| **REST API ingestion: pagination, rate limits, retry, incremental fetch** | `docker/mock-api/app.py` + `ingestion/pipelines/rest_api_campaigns.py` -- verified live across a full 2,190-page sync, incl. a real retry-budget bug found and fixed under load | ✅ |
| **Object storage / flat-file ingestion, messy schema** | `seeders/seed_flatfiles.py` + `ingestion/pipelines/flat_files.py` + `spark_jobs/bronze_to_silver/pos_inventory.py` -- bronze AND silver both verified, exact row-count match, 0 unparseable values | ✅ |
| **Docker Compose, local-first stack** | `docker-compose.yml`, `.env.example` | ✅ |
| **Snowflake-portable SQL / warehouse config** | `dbt/profiles.yml` (Snowflake target, config-only) + `dbt/macros/delta_source.sql` (the one place the engine-specific read is isolated) | ✅ config/boundary built; needs a live Snowflake account to actually run, by design (see README's paid-cloud table) |
| **Schema drift handling** | `ingestion/pipelines/flat_files.py` (bronze absorption) + `spark_jobs/bronze_to_silver/pos_inventory.py` (silver coercion/coalesce) -- both halves verified | ✅ |
| **Late-arriving fact handling** | `spark_jobs/bronze_to_silver/orders.py` (Delta MERGE) + `ops/incident_02_late_arriving_fact.py` (live demo, 2 real bugs found and fixed) | ✅ |
| **Backfill without double-counting** | `ops/incident_03_backfill.py` -- live demo: month deleted, MERGE run twice, exact restore + true no-op second run | ✅ |
| **Delta transaction log / crash recovery** | `ops/incident_04_crash_recovery.py` -- 11.25M-row write genuinely killed mid-flight (SIGKILL to the JVM process group), table proven intact throughout | ✅ |
| **Data-quality gates blocking promotion** | `spark_jobs/quality_gate.py` + `airflow/dags/gold_promotion.py` + `ops/incident_05_quality_gate_block.py` -- live demo through the real DAG: gate failed, `dbt_build_gold` never ran, gold row count unchanged | ✅ |
| **Query tuning / cost & performance** | `ops/incident_08_compaction.py` -- real `OPTIMIZE` run: 3,725 files -> 25 (-99.3%), query time 11.63s -> 1.23s (~9.5x); full "if the bill doubled" write-up in incident-log.md #8 | ✅ |
| **Cross-source referential integrity / real joins** | `seeders/seed_kafka_events.py` (clickstream `order_id` -> live `oltp.orders`) + `powerbi/model/relationships.tmdl` (the same join modelled all the way to the BI layer) | ✅ |
| **Dimensional modelling / SCD2** | `dbt/models/marts/dim_products.sql` (SCD2, surrogate key) + `spark_jobs/bronze_to_silver/products_scd2.py` (the Delta MERGE that builds it) -- 2 live price/status mutations proven | ✅ |
| **Retry/backoff correctness under load** | `ingestion/pipelines/rest_api_campaigns.py` -- a real bug (rate-limit waits sharing the transient-error retry budget) found and fixed during an actual full-volume run, not code review | ✅ |
| **Identity resolution / entity dedup** | `spark_jobs/bronze_to_silver/customers.py` + `dbt/models/staging/stg_customer_identity_map.sql` -- a real downstream bug (1,073 orphaned FK references) found by a dbt test and fixed with a proper remap, not a workaround | ✅ |

## Repo layout

```
docker-compose.yml           Full stack: sources (default profile) + Airflow (orchestration profile)
.env.example                  All config, every credential a local dev default
docker/postgres/init/         DB bootstrap (two logical DBs: oltp + airflow)
docker/mock-api/              Mock ad-platform REST API (SOURCE 2)
docker/airflow/               Custom Airflow image (Java + PySpark + delta-spark + dbt-duckdb + deltalake)
seeders/                      One-off data generators for all 4 sources (seeds the world the platform operates on;
                               NOT part of the orchestrated platform itself)
  seed_postgres_oltp.py       SOURCE 1: ~9.86M rows
  seed_flatfiles.py           SOURCE 3: ~1.87M rows, 4 schema-drift stages
  seed_kafka_events.py        SOURCE 4: ~1.64M events, real cross-source join to Postgres
ingestion/                    ADF-pattern copy-activity layer -- 14.48M rows landed in bronze
  control_table.py            Watermark control table (ingestion.pipeline_watermarks in Postgres)
  delta_writer.py             Shared bronze-write helper (delta-rs, not PySpark -- see BUILD_LOG)
  pipelines/                  postgres_oltp.py, rest_api_campaigns.py, flat_files.py, kafka_events.py
spark_jobs/
  common/spark_session.py     Shared SparkSession builder (Delta + S3A config)
  bronze_to_silver/           9 jobs: customers, stores, products_scd2, orders, order_items_payments,
                               campaign_performance, pos_inventory, clickstream_events
  quality_gate.py             6 checks, blocks silver->gold promotion on failure
dbt/                          silver->gold: 4 dims + 4 facts + measures, 49 tests, seeds/ for CI fixtures
  macros/delta_source.sql     The one engine-specific read (DuckDB/Snowflake/CI-seeds), isolated
airflow/dags/                 bronze_ingestion.py, gold_promotion.py -- both verified success end to end
ops/                          Live "break it and fix it" demo scripts, one per incident-log.md scenario
powerbi/
  model/                      TMDL semantic model source (tables, relationships, roles, measures)
  export_gold_for_powerbi.py  Tested: exports gold -> Parquet for Power BI's zero-driver import path
governance/
  generate_data_dictionary.py Parses dbt's own catalog.json/manifest.json -> docs/DATA_DICTIONARY.md
.github/workflows/ci.yml      sql-lint + dbt-build (seed fixtures) on every PR
docs/
  BUILD_LOG.md                 STAR-format build history, written as components landed
  incident-log.md              8 required scenarios, live-demonstrated, real bugs and fixes
  COMPONENT_MAP.md              this file
  DATA_DICTIONARY.md            generated -- regenerate, don't hand-edit
```

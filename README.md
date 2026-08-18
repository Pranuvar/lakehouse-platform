# Lakehouse Platform -- Fjord Mart Analytics

A local-first, medallion-architecture data platform built on a fictitious
Dublin-headquartered omnichannel retailer ("Fjord Mart"), covering the
full path from four heterogeneous sources through bronze/silver/gold to a
BI semantic layer -- orchestrated, tested, and CI-gated.

This is the second of two portfolio pipelines. The first
([retail-medallion-pipeline](https://github.com/Pranuvar/retail-medallion-pipeline))
covers Postgres → Snowflake → dbt Core → Airflow in depth (incremental
models, SCD2, source freshness, DAG idempotency, query tuning). This one
is deliberately broader: PySpark on Delta Lake, a second orchestration
paradigm, streaming, Power BI with RLS, CI/CD, and governance -- the
technologies the first pipeline doesn't evidence.

Build status: **complete, end to end, all 8 required interview
scenarios live-demonstrated.** All four sources seeded (14.5M+ rows) ->
bronze (14.48M rows, ADF-pattern ingestion) -> silver (9 PySpark jobs:
identity resolution, native Delta `MERGE` SCD2, schema-drift coercion,
dedup) -> a quality gate that provably blocks bad promotions -> gold
(dbt: 4 dims, 4 facts, 49 tests) -> a Power BI semantic model with
dynamic RLS -> CI (GitHub Actions, proven with real exit codes) ->
governance (generated lineage + data dictionary). Every layer wired
into and triggered through the real Airflow DAGs, not run manually and
described. See [docs/BUILD_LOG.md](docs/BUILD_LOG.md) for the detailed,
STAR-format build history -- including every real bug found and fixed
along the way, not just the parts that worked first try -- and
[docs/COMPONENT_MAP.md](docs/COMPONENT_MAP.md) for the full
requirement-by-requirement mapping.

## Architecture

```
  SOURCES (heterogeneous, on purpose)              BRONZE            SILVER           GOLD
 ┌──────────────────────────────────┐          ┌───────────┐   ┌────────────┐   ┌─────────────┐
 │ Postgres OLTP                     │          │           │   │            │   │             │
 │  customers/orders/order_items/... │─┐        │  Delta    │   │   Delta    │   │    Delta    │
 │  (~9.86M rows)                    │ │        │  tables   │   │   tables   │   │  star schema│
 ├────────────────────────────────────┤ │ PySpark│ (raw,    │   │ (cleaned,  │   │ (dims/facts,│
 │ Mock REST API (ad platform)       │ ├───────▶│  append-  │──▶│  conformed,│──▶│   SCD2)     │
 │  paginated, rate-limited,         │ │ bronze │  only,    │   │  deduped)  │   │             │
 │  incremental (~1.1M rows)         │ │        │ partitioned│  │            │   │  dbt Core   │
 ├────────────────────────────────────┤ │        │  on MinIO)│   │  quality   │   │  (silver→   │
 │ MinIO flat files (CSV/Parquet)    │ │        │           │   │  gates     │   │   gold)     │
 │  POS inventory, schema drift      │─┤        └───────────┘   │  BLOCK     │   └──────┬──────┘
 │  (~1.87M rows)                    │ │                        │  promotion │          │
 ├────────────────────────────────────┤ │                        └────────────┘          │
 │ Redpanda (Kafka API) clickstream  │ │                                                 ▼
 │  append-only, real join to orders │─┘                                          ┌─────────────┐
 │  (~1.64M events)                  │                                            │  Power BI   │
 └──────────────────────────────────┘                                            │  semantic   │
                                                                                   │  model +    │
        ▲ ingestion orchestrated by:                                             │  DAX + RLS  │
        │  Airflow (LocalExecutor)                                                └─────────────┘
        │  + an ADF-pattern metadata-driven pipeline layer
        │  (YAML pipeline defs + a copy-activity runner, mimicking
        │    linked services / pipelines / triggers)
        └── governance: dbt docs lineage graph + generated data dictionary
            CI/CD: GitHub Actions runs dbt build/test + sqlfluff on every PR
```

**Why bronze is PySpark and silver→gold is dbt, on purpose:** bronze needs
to absorb messy, heterogeneous, occasionally schema-drifting input from
four different transport mechanisms (JDBC, REST, object storage, Kafka) --
that's a data-engineering problem PySpark is built for. Once everything is
conformed Delta tables with a stable contract, silver→gold is a modelling
problem -- SQL, tests, docs, lineage -- which is exactly dbt's job. Being
able to speak to both engines, and to explain *why* the boundary sits
where it does, is the point of this project.

## What's local-free vs. what needs paid cloud

| Component | Local (default) | Paid cloud equivalent |
|---|---|---|
| Warehouse (dbt target) | **DuckDB** file, zero cost | Snowflake -- `profiles.yml` target swap only, SQL is written adapter-agnostic on purpose |
| Object storage / Delta lake | **MinIO** (S3-compatible) | AWS S3 / ADLS -- same `s3a://` code path |
| Orchestrated ingestion (ADF pattern) | Metadata-driven pipeline runner, orchestrated by Airflow | Azure Data Factory -- needs an Azure subscription; the local runner mirrors ADF's linked-service/pipeline/trigger model so the mapping is direct |
| Event streaming | **Redpanda** (Kafka-API compatible, single container) | Confluent Cloud / MSK -- same producer/consumer code, bootstrap-server change only |
| BI semantic model + RLS | **Power BI Desktop** (free), RLS tested via "View As Roles" | Power BI Service (Pro/tenant) needed only to *publish* and test live RLS for other named users -- not needed to build or demo the model |

Nothing in this build requires a paid account to run end to end.

## Quickstart

```bash
cp .env.example .env
docker compose up -d           # sources only: postgres, minio, redpanda, mock-api
```

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-seed.txt
cd seeders
python seed_postgres_oltp.py   # ~9.86M rows, ~100s
python seed_flatfiles.py       # ~1.87M rows across 3,725 files, ~60s
python seed_kafka_events.py    # ~1.64M events, ~110s (run OLTP seeder first -- it joins to real orders)
```

Bring up orchestration once sources are seeded (heavier image, builds
Java + PySpark + dbt on first run):

```bash
docker compose --profile orchestration up -d --build
# Airflow UI: http://localhost:8080  (admin/admin)
```

New DAGs are paused on creation (Airflow's own default) -- unpause and
trigger the bronze ingestion DAG, which runs all 4 source-to-bronze copy
activities in parallel:

```bash
docker compose --profile orchestration exec airflow-scheduler airflow dags unpause bronze_ingestion
docker compose --profile orchestration exec airflow-scheduler airflow dags trigger bronze_ingestion
# watch it in the UI, or:
docker compose --profile orchestration exec airflow-scheduler airflow dags list-runs -d bronze_ingestion
```

Each pipeline is also directly runnable/debuggable outside the DAG:

```bash
docker compose --profile orchestration exec airflow-scheduler \
  python /opt/airflow/ingestion/pipelines/postgres_oltp.py
```

**Bronze -> silver** (PySpark, one job per source-shape -- run
`customers`/`stores`/`products_scd2` before `order_items_payments`,
which checks referential integrity against silver.orders):

```bash
for job in customers stores products_scd2 orders order_items_payments \
           campaign_performance pos_inventory clickstream_events; do
  docker compose --profile orchestration exec airflow-scheduler \
    python /opt/airflow/spark_jobs/bronze_to_silver/$job.py
done
```

**Silver -> gold**, gated (`gold_promotion` DAG -- a failed quality gate
blocks the dbt task entirely, see docs/incident-log.md #5):

```bash
docker compose --profile orchestration exec airflow-scheduler airflow dags unpause gold_promotion
docker compose --profile orchestration exec airflow-scheduler airflow dags trigger gold_promotion -o json
```

**Live "break it and fix it" demos** -- one script per required
interview scenario, each reproducible on demand (see
[docs/incident-log.md](docs/incident-log.md) for what each one proves):

```bash
docker compose --profile orchestration exec airflow-scheduler python /opt/airflow/ops/incident_02_late_arriving_fact.py
docker compose --profile orchestration exec airflow-scheduler python /opt/airflow/ops/incident_03_backfill.py
docker compose --profile orchestration exec airflow-scheduler python /opt/airflow/ops/incident_04_crash_recovery.py
docker compose --profile orchestration exec airflow-scheduler python /opt/airflow/ops/incident_05_quality_gate_block.py
docker compose --profile orchestration exec airflow-scheduler python /opt/airflow/ops/incident_08_compaction.py
```

**Power BI export + governance** (both tested and real, not aspirational):

```bash
docker compose --profile orchestration exec airflow-scheduler python /opt/airflow/powerbi/export_gold_for_powerbi.py
docker compose --profile orchestration exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate"
docker compose --profile orchestration exec airflow-scheduler python /opt/airflow/governance/generate_data_dictionary.py
```

**CI, run locally with the exact commands GitHub Actions runs** (no
live infra needed -- see `.github/workflows/ci.yml` and
docs/incident-log.md #6 for why seed fixtures, not the real stack):

```bash
cd dbt && dbt deps
DBT_PROFILES_DIR=$(pwd) DBT_TARGET=local dbt build --vars '{use_seeds: true}'
```

Sanity-check the mock API directly:

```bash
curl -H "X-API-Key: dev-local-key-do-not-use-in-prod" \
  "http://localhost:8000/v1/campaigns/performance?page=1&page_size=5"
```

MinIO console: http://localhost:9001 (credentials in `.env`). Airflow
UI: http://localhost:8080 (admin/admin).

## What this demonstrates 

A single project that shows, with working code and a documented incident
trail rather than a slide:

- **Ingesting from genuinely different systems** the way a real platform
  has to -- a transactional database, a rate-limited third-party API, an
  unmanaged flat-file drop zone with drifting schema, and an append-only
  event stream -- and handling each one's specific failure modes (retry/
  backoff, schema evolution, deduplication, out-of-order events) rather
  than treating "ingestion" as one generic problem.
- **Two processing engines used where each is actually the right tool,
  twice over**: PySpark for messy raw-to-conformed transformation and
  native Delta `MERGE`-based SCD2/upserts, dbt for tested, documented,
  version-controlled business logic and its own (deliberately singular)
  incremental model -- and a third, JVM-free engine (delta-rs) for
  lightweight ingestion, mirroring how Azure Data Factory's Copy
  Activity isn't Spark either. Three engines, each earning its place,
  not three boxes ticked.
- **Orchestration and governance as first-class**, not afterthoughts: an
  Airflow DAG proven idempotent under a real backfill (delete a month,
  rebuild it twice, zero double-counting), a quality gate that provably
  blocks a corrupt promotion through the real orchestrator (not a script
  asserting the concept), CI that fails on the exact real exit code
  GitHub Actions would see, and a generated (not hand-maintained) data
  dictionary + lineage graph.
- **Cost- and constraint-awareness backed by real numbers**: Redpanda
  over Kafka+ZK for a laptop-class RAM budget, bulk-load-then-constrain
  for a 9.7M-row load in 97 seconds, and a live `OPTIMIZE` compaction run
  that cut 3,725 files to 25 and a real query from 11.6s to 1.2s -- see
  [docs/BUILD_LOG.md](docs/BUILD_LOG.md) for the reasoning behind every
  one of these, not just the outcome.
- **A build process that finds and fixes its own bugs on camera, over
  and over**: the Redpanda dual-listener fix, a Spark lazy-evaluation
  trap that silently mis-reported a correct SCD2 write, a seed-data bug
  that raced a watermark into the future, a Delta MERGE ambiguity
  exposed by a legitimate incident-recovery action, an SCD2 backdating
  bug that nulled 40% of a fact table's margin, a VARCHAR/TIMESTAMP
  mismatch caught only by a genuine (non-full-refresh) incremental run
  -- all eight required interview scenarios in
  [docs/incident-log.md](docs/incident-log.md) are answered with a real
  run and, in most cases, a real bug found building the demo itself, not
  a description of what would probably happen.

## Repo map

See [docs/COMPONENT_MAP.md](docs/COMPONENT_MAP.md) for the full
file-to-requirement mapping.

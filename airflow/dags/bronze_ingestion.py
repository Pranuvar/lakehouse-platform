"""
bronze_ingestion -- the orchestration layer over the four ADF-pattern
copy activities in ingestion/pipelines/. This DAG's only job is to run
each pipeline's `run()` and let Airflow's LocalExecutor handle
parallelism, retries-at-the-task-level, and observability (logs, task
duration, success/failure history) -- the actual extraction/retry/
incremental logic lives in ingestion/, not here, on purpose: that logic
is engine-agnostic (it would run the same under any orchestrator), so it
shouldn't be entangled with Airflow-specific code.

All four tasks are independent (no `>>` chains) -- they land into
different bronze tables and don't depend on each other's output, so
LocalExecutor runs them concurrently rather than the DAG serialising
work that doesn't need to be serial.

Scheduling: `schedule=None` (manually/API triggered) for this build,
deliberately -- the four sources don't actually share a natural cadence
in a real deployment (Postgres/REST API: hourly-ish; flat-file drops:
whenever the till batch-uploads, in practice daily; Kafka: every few
minutes for near-real-time freshness), so a single fixed
`schedule_interval` on one DAG would be the wrong shape for production.
The honest production version is four DAGs (or one DAG templated per
source) each on its own schedule, sharing the same ingestion/ pipeline
code -- not a limitation of the code, a scheduling decision left
explicit rather than hidden behind one arbitrary cron string.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_postgres_oltp(**_):
    from ingestion.pipelines.postgres_oltp import run
    return run()


def _run_rest_api_campaigns(**_):
    from ingestion.pipelines.rest_api_campaigns import run
    return run()


def _run_flat_files(**_):
    from ingestion.pipelines.flat_files import run
    return run()


def _run_kafka_events(**_):
    from ingestion.pipelines.kafka_events import run
    return run()


with DAG(
    dag_id="bronze_ingestion",
    description="ADF-pattern copy activities: 4 heterogeneous sources -> bronze Delta (MinIO)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion", "adf-pattern"],
) as dag:
    PythonOperator(task_id="postgres_oltp_to_bronze", python_callable=_run_postgres_oltp)
    PythonOperator(task_id="rest_api_campaigns_to_bronze", python_callable=_run_rest_api_campaigns)
    PythonOperator(task_id="flat_files_to_bronze", python_callable=_run_flat_files)
    PythonOperator(task_id="kafka_events_to_bronze", python_callable=_run_kafka_events)

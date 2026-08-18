"""
gold_promotion -- the DAG that makes the quality gate an actual GATE,
not just a script that exists. Two tasks, one dependency:

    quality_gate >> dbt_build_gold

`quality_gate` runs spark_jobs/quality_gate.py via spark-submit and
exits non-zero if any check fails. Airflow's DEFAULT trigger rule
(`all_success`) means `dbt_build_gold` only runs if `quality_gate`
actually succeeded -- a failed gate leaves it `upstream_failed`, never
executed, gold tables completely untouched. This is the literal
mechanism behind "quality gates that block promotion, not tests that
report after the fact": the blocking isn't a policy or a convention,
it's what happens by default when one task depends on another in
Airflow and nothing overrides the trigger rule.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="gold_promotion",
    description="Quality gate -> dbt gold build. A failed gate blocks the dbt task entirely.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "quality-gate", "dbt"],
) as dag:
    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command="python /opt/airflow/spark_jobs/quality_gate.py",
    )

    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command="cd /opt/airflow/dbt && dbt build",
    )

    quality_gate >> dbt_build_gold

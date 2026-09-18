"""
ecommerce_analytics_dag
========================

Orchestrates a daily batch pipeline:

    extract_raw_data  ->  load_to_warehouse  ->  dbt_deps  ->  dbt_run  ->  dbt_test  ->  dbt_docs_generate

This mirrors a real vendor-data-to-warehouse pattern (e.g. the author's
production GDPR reporting pipeline: Python extraction + Spark transforms +
Airflow orchestration, with upstream data checks and automated retries so
failures alert on-call rather than fail silently).

Design notes:
- `retries` + `retry_delay` + `on_failure_callback` reproduce the
  "automated retries and failure alerts" pattern from that pipeline.
- `dbt_test` is a hard gate: if data-quality tests fail, `dbt_docs_generate`
  and any downstream BI-refresh task never run, so bad data never reaches
  a dashboard.
- Each step is a small, independently testable Python/Bash call rather than
  one large task, so a failure is immediately attributable to one stage.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
import os

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt_project")


def alert_on_failure(context):
    """
    Stand-in for a real Slack/PagerDuty/email alert integration.
    In production this would post to a webhook; here it logs loudly so the
    behavior is visible in the task log during a demo run.
    """
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exec_date = context["ts"]
    print(
        f"[ALERT] Task '{task_id}' in DAG '{dag_id}' failed for run {exec_date}. "
        f"Paging on-call (simulated)."
    )


default_args = {
    "owner": "zoe.chen",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
    "email_on_failure": False,
}

with DAG(
    dag_id="ecommerce_analytics_pipeline",
    description="Daily extract -> load -> dbt transform/test pipeline for the e-commerce analytics mart",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # 06:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["analytics", "dbt", "portfolio"],
) as dag:

    def _extract_raw_data():
        import generate_mock_data
        generate_mock_data.run()

    def _load_to_warehouse():
        import load_raw_to_duckdb
        load_raw_to_duckdb.run()

    extract_raw_data = PythonOperator(
        task_id="extract_raw_data",
        python_callable=_extract_raw_data,
    )

    load_to_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=_load_to_warehouse,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --target ci",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --target ci",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --target ci",
    )

    extract_raw_data >> load_to_warehouse >> dbt_deps >> dbt_run >> dbt_test >> dbt_docs_generate

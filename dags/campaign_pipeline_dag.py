"""
Airflow DAG for the AdPulse pipeline.

Chains: extract -> load_staging -> transform -> load_warehouse -> validate

Each task retries twice with a 5 minute delay before failing the run.
If validate.py finds a critical data-quality issue it exits non-zero,
which fails the task (and the run) -- the DAG deliberately does not
mark the run successful just because rows loaded; a clean load AND a
passing validation are both required.

To use: copy this file into your Airflow DAGS_FOLDER, and make sure the
`adpulse` package (the src/ directory) is importable on the Airflow
workers -- e.g. by installing it with `pip install -e .` from the repo
root, or adding the repo root to PYTHONPATH.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "adpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _run_extract():
    from src.extract import extract
    extract()


def _run_load_staging():
    from src.load_staging import load_staging
    load_staging()


def _run_transform():
    from src.transform import transform
    transform()


def _run_load_warehouse():
    from src.load_warehouse import load_warehouse
    load_warehouse()


def _run_validate():
    from src.validate import validate
    ok = validate()
    if not ok:
        raise RuntimeError("Data-quality validation failed a critical check -- see logs/ for the report.")


with DAG(
    dag_id="adpulse_campaign_pipeline",
    description="Extract, load, transform, model, and validate ad-campaign performance data",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["adpulse", "etl", "marketing-analytics"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=_run_extract,
    )

    load_staging_task = PythonOperator(
        task_id="load_staging",
        python_callable=_run_load_staging,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=_run_transform,
    )

    load_warehouse_task = PythonOperator(
        task_id="load_warehouse",
        python_callable=_run_load_warehouse,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=_run_validate,
    )

    extract_task >> load_staging_task >> transform_task >> load_warehouse_task >> validate_task

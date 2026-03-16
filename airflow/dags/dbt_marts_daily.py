# DAG: dbt_marts_daily
# Builds final mart models consumed by dashboards and reports.
# Models: dim_users, dim_merchants, fct_payments, rpt_daily_payment_volume
# Schedule: 12:30 UTC daily (after dbt_intermediate_daily)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import DBT_IMAGE, DOCKER_DEFAULTS

with DAG(
    dag_id="dbt_marts_daily",
    description="Run dbt mart models (dimensions, facts, reports)",
    schedule="30 12 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "fintech-wiorx", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fintech", "dbt"],
) as dag:

    dbt_marts = DockerOperator(
        task_id="dbt_marts",
        image=DBT_IMAGE,
        command="dbt run --select marts",   # dims + facts + reports
        working_dir="/usr/app",
        environment={"DBT_ENV": "dev"},
        **DOCKER_DEFAULTS,
    )

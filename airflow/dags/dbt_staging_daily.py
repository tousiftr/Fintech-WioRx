# DAG: dbt_staging_daily
# Materialises staging views on top of raw Postgres tables.
# Models: stg_users, stg_merchants, stg_payments, stg_product_events
# Schedule: 08:00 UTC daily (after data_load_daily)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import DBT_IMAGE, DOCKER_DEFAULTS

with DAG(
    dag_id="dbt_staging_daily",
    description="Run dbt staging models",
    schedule="0 8 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "fintech-wiorx", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fintech", "dbt"],
) as dag:

    dbt_staging = DockerOperator(
        task_id="dbt_staging",
        image=DBT_IMAGE,
        command="dbt run --select staging",     # stg_* views only
        working_dir="/usr/app",
        environment={"DBT_ENV": "dev"},
        **DOCKER_DEFAULTS,
    )

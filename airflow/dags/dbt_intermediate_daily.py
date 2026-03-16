# DAG: dbt_intermediate_daily
# Materialises intermediate tables that enrich and join staging data.
# Models: int_payments_enriched, int_user_activity, int_merchant_metrics
# Schedule: 12:00 UTC daily (after dbt_staging_daily)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import DBT_IMAGE, DOCKER_DEFAULTS

with DAG(
    dag_id="dbt_intermediate_daily",
    description="Run dbt intermediate models",
    schedule="0 12 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "fintech-wiorx", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fintech", "dbt"],
) as dag:

    dbt_intermediate = DockerOperator(
        task_id="dbt_intermediate",
        image=DBT_IMAGE,
        command="dbt run --select intermediate",  # int_* tables only
        working_dir="/usr/app",
        environment={"DBT_ENV": "dev"},
        **DOCKER_DEFAULTS,
    )

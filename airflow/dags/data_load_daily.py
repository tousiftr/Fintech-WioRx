# DAG: data_load_daily
# Reads today parquet files from MinIO and upserts into raw.* Postgres tables.
# Schedule: 07:00 UTC daily (1 hr after data_generate_daily)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import GENERATOR_IMAGE, MINIO_ENV, DATABASE_URL, DOCKER_DEFAULTS

with DAG(
    dag_id="data_load_daily",
    description="Load parquet from MinIO into raw Postgres tables",
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "fintech-wiorx", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fintech", "ingestion"],
) as dag:

    load_to_postgres = DockerOperator(
        task_id="load_to_postgres",
        image=GENERATOR_IMAGE,
        command="python load_to_postgres.py",   # MinIO -> raw.* upsert
        environment={**MINIO_ENV, "DATABASE_URL": DATABASE_URL},
        **DOCKER_DEFAULTS,
    )

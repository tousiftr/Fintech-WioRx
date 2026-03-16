# DAG: data_generate_daily
# Generates synthetic fintech data (users, merchants, payments, events)
# and uploads partitioned parquet files to MinIO.
# Schedule: 06:00 UTC daily
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import GENERATOR_IMAGE, MINIO_ENV, DOCKER_DEFAULTS

with DAG(
    dag_id="data_generate_daily",
    description="Generate synthetic data and upload to MinIO",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "fintech-wiorx", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fintech", "ingestion"],
) as dag:

    generate_data = DockerOperator(
        task_id="generate_data",
        image=GENERATOR_IMAGE,
        command="python main.py",       # generates & uploads to MinIO
        environment=MINIO_ENV,
        **DOCKER_DEFAULTS,
    )

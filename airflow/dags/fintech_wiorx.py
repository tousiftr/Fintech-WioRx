from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'fintech-wiorx',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

NETWORK = 'fintech-wiorx_default'
GENERATOR_IMAGE = 'fintech-wiorx-generator:latest'
DBT_IMAGE = 'fintech-wiorx-dbt:latest'

MINIO_ENV = {
    'MINIO_ENDPOINT': 'http://minio:9000',
    'MINIO_ROOT_USER': 'minioadmin',
    'MINIO_ROOT_PASSWORD': 'minioadmin123',
    'MINIO_BUCKET': 'fintech-wiorx-lake',
}

with DAG(
    dag_id='fintech_wiorx',
    description='Generate data, load to Postgres raw, run dbt',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['fintech', 'wiorx'],
) as dag:

    generate_data = DockerOperator(
        task_id='generate_data',
        image=GENERATOR_IMAGE,
        command='python main.py',
        network_mode=NETWORK,
        environment=MINIO_ENV,
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
    )

    load_to_postgres = DockerOperator(
        task_id='load_to_postgres',
        image=GENERATOR_IMAGE,
        command='python load_to_postgres.py',
        network_mode=NETWORK,
        environment={
            **MINIO_ENV,
            'DATABASE_URL': 'postgresql://fintech:fintech123@postgres:5432/fintech',
        },
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
    )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        image=DBT_IMAGE,
        command='dbt run',
        working_dir='/usr/app',
        network_mode=NETWORK,
        environment={'DBT_ENV': 'dev'},
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
    )

    generate_data >> load_to_postgres >> dbt_run

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'fintech-wiorx',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_intermediate',
    description='Run dbt intermediate models only',
    schedule='0 12 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['fintech', 'dbt'],
) as dag:

    dbt_intermediate = DockerOperator(
        task_id='dbt_intermediate',
        image='fintech-wiorx-dbt:latest',
        command='dbt run --select intermediate',
        working_dir='/usr/app',
        network_mode='fintech-wiorx_default',
        environment={'DBT_ENV': 'dev'},
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
    )

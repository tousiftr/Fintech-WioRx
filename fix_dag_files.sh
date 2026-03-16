#!/usr/bin/env bash
# Run this once to fix DAG file ownership and apply all corrections
set -e

DAGS_DIR="$(dirname "$0")/airflow/dags"

echo "==> Fixing ownership of DAG files..."
sudo chown -R "$(id -u):$(id -g)" "$DAGS_DIR"

echo "==> Writing config.py..."
cat > "$DAGS_DIR/config.py" << 'EOF'
# Shared constants used across all Fintech DAGs
import os

# Docker images (built by docker compose)
GENERATOR_IMAGE = "fintech-wiorx-generator:latest"
DBT_IMAGE       = "fintech-wiorx-dbt:latest"

# All containers join this network to reach Postgres, MinIO by hostname
NETWORK = "fintech-wiorx_default"

# MinIO (data lake) connection settings
MINIO_ENV = {
    "MINIO_ENDPOINT":      "http://minio:9000",
    "MINIO_ROOT_USER":     "minioadmin",
    "MINIO_ROOT_PASSWORD": "minioadmin123",
    "MINIO_BUCKET":        "fintech-wiorx-lake",
}

# Postgres raw layer connection
DATABASE_URL = "postgresql://fintech:fintech123@postgres:5432/fintech"

# dbt Postgres connection vars -- read from env (loaded via dbt/.env in docker-compose)
DBT_ENV = {
    "DBT_ENV":      os.environ.get("DBT_ENV", "dev"),
    "PG_HOST":      os.environ.get("PG_HOST", ""),
    "PG_PORT":      os.environ.get("PG_PORT", "5432"),
    "PG_DB":        os.environ.get("PG_DB", ""),
    "PG_SCHEMA":    os.environ.get("PG_SCHEMA", "analytics"),
    "PG_USER":      os.environ.get("PG_USER", ""),
    "PG_PASSWORD":  os.environ.get("PG_PASSWORD", ""),
    "PG_THREADS":   os.environ.get("PG_THREADS", "4"),
}

# Common DockerOperator kwargs shared by all tasks
DOCKER_DEFAULTS = {
    "network_mode": NETWORK,
    "auto_remove":  "success",
    "docker_url":   "unix:///var/run/docker.sock",
}
EOF

echo "==> Writing dbt_staging_daily.py..."
cat > "$DAGS_DIR/dbt_staging_daily.py" << 'EOF'
# DAG: dbt_staging_daily
# Materialises staging views on top of raw Postgres tables.
# Models: stg_users, stg_merchants, stg_payments, stg_product_events
# Schedule: 08:00 UTC daily (after data_load_daily)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import DBT_IMAGE, DBT_ENV, DOCKER_DEFAULTS

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
        command="dbt run --select staging",
        working_dir="/usr/app",
        environment=DBT_ENV,
        **DOCKER_DEFAULTS,
    )
EOF

echo "==> Writing dbt_intermediate_daily.py..."
cat > "$DAGS_DIR/dbt_intermediate_daily.py" << 'EOF'
# DAG: dbt_intermediate_daily
# Materialises intermediate tables that enrich and join staging data.
# Models: int_payments_enriched, int_user_activity, int_merchant_metrics
# Schedule: 12:00 UTC daily (after dbt_staging_daily)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import DBT_IMAGE, DBT_ENV, DOCKER_DEFAULTS

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
        command="dbt run --select intermediate",
        working_dir="/usr/app",
        environment=DBT_ENV,
        **DOCKER_DEFAULTS,
    )
EOF

echo "==> Writing dbt_marts_daily.py..."
cat > "$DAGS_DIR/dbt_marts_daily.py" << 'EOF'
# DAG: dbt_marts_daily
# Builds final mart models consumed by dashboards and reports.
# Models: dim_users, dim_merchants, fct_payments, rpt_daily_payment_volume
# Schedule: 12:30 UTC daily (after dbt_intermediate_daily)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import DBT_IMAGE, DBT_ENV, DOCKER_DEFAULTS

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
        command="dbt run --select marts",
        working_dir="/usr/app",
        environment=DBT_ENV,
        **DOCKER_DEFAULTS,
    )
EOF

echo "==> Writing pipeline_realtime.py..."
cat > "$DAGS_DIR/pipeline_realtime.py" << 'EOF'
# DAG: pipeline_realtime
# Every 10 minutes:
#   1. realtime_load  — generates 10-20 new users + 80-150 payments with NOW timestamps,
#                       inserts directly into Postgres (no MinIO needed)
#   2. dbt_staging    — refreshes staging views so data is immediately queryable
#
# max_active_runs=1 prevents runs overlapping if a cycle takes longer than 10 min.
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from config import GENERATOR_IMAGE, DBT_IMAGE, DATABASE_URL, DBT_ENV, DOCKER_DEFAULTS

with DAG(
    dag_id="pipeline_realtime",
    description="Ingest new fintech data directly to Postgres every 10 min",
    schedule="*/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "fintech-wiorx", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["fintech", "realtime"],
) as dag:

    realtime_load = DockerOperator(
        task_id="realtime_load",
        image=GENERATOR_IMAGE,
        command="python realtime_load.py",
        environment={"DATABASE_URL": DATABASE_URL},
        **DOCKER_DEFAULTS,
    )

    dbt_staging = DockerOperator(
        task_id="dbt_staging",
        image=DBT_IMAGE,
        command="dbt run --select staging",
        working_dir="/usr/app",
        environment=DBT_ENV,
        **DOCKER_DEFAULTS,
    )

    realtime_load >> dbt_staging
EOF

echo "==> All DAG files updated successfully."

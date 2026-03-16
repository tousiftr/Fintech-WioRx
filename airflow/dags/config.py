# Shared constants used across all Fintech DAGs
import os

# Docker images (built by docker compose)
GENERATOR_IMAGE = "fintech-wiorx-generator:latest"
DBT_IMAGE       = "fintech-wiorx-dbt:latest"

# All containers join this network to reach Postgres, MinIO by hostname
NETWORK = "fintech-wiorx_default"

# MinIO (data lake) connection settings
MINIO_ENV = {
    "MINIO_ENDPOINT":      os.environ.get("MINIO_ENDPOINT",      "http://minio:9000"),
    "MINIO_ROOT_USER":     os.environ.get("MINIO_ROOT_USER",     "minioadmin"),
    "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123"),
    "MINIO_BUCKET":        os.environ.get("MINIO_BUCKET",        "fintech-wiorx-lake"),
}

# Postgres raw layer connection
DATABASE_URL = "postgresql://fintech:fintech123@postgres:5432/fintech"

# dbt Postgres connection vars -- read from env (loaded via dbt/.env in docker-compose)
DBT_ENV = {
    "DBT_ENV":     os.environ.get("DBT_ENV",     "dev"),
    "PG_HOST":     os.environ.get("PG_HOST",     ""),
    "PG_PORT":     os.environ.get("PG_PORT",     "5432"),
    "PG_DB":       os.environ.get("PG_DB",       ""),
    "PG_SCHEMA":   os.environ.get("PG_SCHEMA",   "analytics"),
    "PG_USER":     os.environ.get("PG_USER",     ""),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD", ""),
    "PG_THREADS":  os.environ.get("PG_THREADS",  "4"),
}

# Common DockerOperator kwargs shared by all tasks
DOCKER_DEFAULTS = {
    "network_mode": NETWORK,
    "auto_remove":  "success",
    "docker_url":   "unix:///var/run/docker.sock",
}

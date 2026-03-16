# Shared constants used across all Fintech DAGs

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

# Common DockerOperator kwargs shared by all tasks
DOCKER_DEFAULTS = {
    "network_mode": NETWORK,
    "auto_remove":  "success",
    "docker_url":   "unix://var/run/docker.sock",
}

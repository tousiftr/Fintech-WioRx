FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-postgres==1.9.0

WORKDIR /usr/app

COPY dbt/ /usr/app/

ENV DBT_PROFILES_DIR=/usr/app/profiles

CMD ["dbt", "--version"]

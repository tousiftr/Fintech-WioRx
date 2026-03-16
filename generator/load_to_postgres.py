import os
import io
import uuid
from datetime import date, datetime

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

from datetime import datetime, timezone

NOW       = datetime.now(timezone.utc)
RUN_BATCH = os.getenv("RUN_BATCH", NOW.strftime("%Y%m%d_%H%M%S"))
RUN_DATE  = RUN_BATCH[:8]

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT",    "http://minio:9000")
MINIO_ROOT_USER   = os.getenv("MINIO_ROOT_USER",   "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET",      "fintech-wiorx-lake")
DATABASE_URL      = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in environment variables.")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ROOT_USER,
    aws_secret_access_key=MINIO_ROOT_PASSWORD,
)

engine = create_engine(DATABASE_URL)


def load_parquet_from_minio(dataset_name: str):
    key = f"raw/{dataset_name}/date={RUN_DATE}/batch={RUN_BATCH}/{dataset_name}.parquet"
    print(f"Reading from S3: s3://{MINIO_BUCKET}/{key}")
    obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
    data = obj["Body"].read()
    df = pd.read_parquet(io.BytesIO(data))
    return df, key


def insert_users(df: pd.DataFrame, source_file: str, batch_id: str, ingested_at: datetime) -> None:
    insert_sql = text("""
        INSERT INTO raw.users (
            user_id, created_at, country, acquisition_channel, kyc_status, device_type,
            source_file, ingested_at, batch_id
        )
        VALUES (
            :user_id, :created_at, :country, :acquisition_channel, :kyc_status, :device_type,
            :source_file, :ingested_at, :batch_id
        )
        ON CONFLICT (user_id) DO UPDATE SET
            created_at = EXCLUDED.created_at,
            country = EXCLUDED.country,
            acquisition_channel = EXCLUDED.acquisition_channel,
            kyc_status = EXCLUDED.kyc_status,
            device_type = EXCLUDED.device_type,
            source_file = EXCLUDED.source_file,
            ingested_at = EXCLUDED.ingested_at,
            batch_id = EXCLUDED.batch_id
    """)

    rows = []
    for _, row in df.iterrows():
        created_at = row["created_at"]
        if pd.notnull(created_at) and hasattr(created_at, "to_pydatetime"):
            created_at = created_at.to_pydatetime()

        rows.append({
            "user_id": row["user_id"],
            "created_at": created_at if pd.notnull(created_at) else None,
            "country": row["country"],
            "acquisition_channel": row["acquisition_channel"],
            "kyc_status": row["kyc_status"],
            "device_type": row["device_type"],
            "source_file": source_file,
            "ingested_at": ingested_at,
            "batch_id": batch_id,
        })

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    print(f"Upserted {len(rows)} rows into raw.users")


def insert_merchants(df: pd.DataFrame, source_file: str, batch_id: str, ingested_at: datetime) -> None:
    insert_sql = text("""
        INSERT INTO raw.merchants (
            merchant_id, merchant_name, category, country, created_at,
            source_file, ingested_at, batch_id
        )
        VALUES (
            :merchant_id, :merchant_name, :category, :country, :created_at,
            :source_file, :ingested_at, :batch_id
        )
        ON CONFLICT (merchant_id) DO UPDATE SET
            merchant_name = EXCLUDED.merchant_name,
            category = EXCLUDED.category,
            country = EXCLUDED.country,
            created_at = EXCLUDED.created_at,
            source_file = EXCLUDED.source_file,
            ingested_at = EXCLUDED.ingested_at,
            batch_id = EXCLUDED.batch_id
    """)

    rows = []
    for _, row in df.iterrows():
        created_at = row["created_at"]
        if pd.notnull(created_at) and hasattr(created_at, "to_pydatetime"):
            created_at = created_at.to_pydatetime()

        rows.append({
            "merchant_id": row["merchant_id"],
            "merchant_name": row["merchant_name"],
            "category": row["category"],
            "country": row["country"],
            "created_at": created_at if pd.notnull(created_at) else None,
            "source_file": source_file,
            "ingested_at": ingested_at,
            "batch_id": batch_id,
        })

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    print(f"Upserted {len(rows)} rows into raw.merchants")


def insert_payments(df: pd.DataFrame, source_file: str, batch_id: str, ingested_at: datetime) -> None:
    insert_sql = text("""
        INSERT INTO raw.payments (
            payment_id, user_id, merchant_id, amount, currency, payment_method, gateway, status, created_at,
            source_file, ingested_at, batch_id
        )
        VALUES (
            :payment_id, :user_id, :merchant_id, :amount, :currency, :payment_method, :gateway, :status, :created_at,
            :source_file, :ingested_at, :batch_id
        )
        ON CONFLICT (payment_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            merchant_id = EXCLUDED.merchant_id,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            payment_method = EXCLUDED.payment_method,
            gateway = EXCLUDED.gateway,
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            source_file = EXCLUDED.source_file,
            ingested_at = EXCLUDED.ingested_at,
            batch_id = EXCLUDED.batch_id
    """)

    rows = []
    for _, row in df.iterrows():
        created_at = row["created_at"]
        if pd.notnull(created_at) and hasattr(created_at, "to_pydatetime"):
            created_at = created_at.to_pydatetime()

        amount = row["amount"]
        amount = None if pd.isnull(amount) else amount

        rows.append({
            "payment_id": row["payment_id"],
            "user_id": row["user_id"],
            "merchant_id": row["merchant_id"],
            "amount": amount,
            "currency": row["currency"],
            "payment_method": row["payment_method"],
            "gateway": row["gateway"],
            "status": row["status"],
            "created_at": created_at if pd.notnull(created_at) else None,
            "source_file": source_file,
            "ingested_at": ingested_at,
            "batch_id": batch_id,
        })

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    print(f"Upserted {len(rows)} rows into raw.payments")


def insert_product_events(df: pd.DataFrame, source_file: str, batch_id: str, ingested_at: datetime) -> None:
    insert_sql = text("""
        INSERT INTO raw.product_events (
            event_id, event_name, event_timestamp, user_id, payment_id, session_id, platform, country,
            source_file, ingested_at, batch_id
        )
        VALUES (
            :event_id, :event_name, :event_timestamp, :user_id, :payment_id, :session_id, :platform, :country,
            :source_file, :ingested_at, :batch_id
        )
        ON CONFLICT (event_id) DO UPDATE SET
            event_name = EXCLUDED.event_name,
            event_timestamp = EXCLUDED.event_timestamp,
            user_id = EXCLUDED.user_id,
            payment_id = EXCLUDED.payment_id,
            session_id = EXCLUDED.session_id,
            platform = EXCLUDED.platform,
            country = EXCLUDED.country,
            source_file = EXCLUDED.source_file,
            ingested_at = EXCLUDED.ingested_at,
            batch_id = EXCLUDED.batch_id
    """)

    rows = []
    for _, row in df.iterrows():
        event_timestamp = row["event_timestamp"]
        if pd.notnull(event_timestamp) and hasattr(event_timestamp, "to_pydatetime"):
            event_timestamp = event_timestamp.to_pydatetime()

        rows.append({
            "event_id": row["event_id"],
            "event_name": row["event_name"],
            "event_timestamp": event_timestamp if pd.notnull(event_timestamp) else None,
            "user_id": row["user_id"],
            "payment_id": row["payment_id"] if pd.notnull(row["payment_id"]) else None,
            "session_id": row["session_id"] if pd.notnull(row["session_id"]) else None,
            "platform": row["platform"],
            "country": row["country"],
            "source_file": source_file,
            "ingested_at": ingested_at,
            "batch_id": batch_id,
        })

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    print(f"Upserted {len(rows)} rows into raw.product_events")


def process_dataset(dataset_name: str, insert_fn):
    print(f"Starting load for {dataset_name}...")
    df, source_file = load_parquet_from_minio(dataset_name)
    print(f"Loaded {dataset_name} parquet with {len(df)} rows")

    batch_id = f"{dataset_name}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    ingested_at = datetime.utcnow()

    print("source_file:", source_file)
    print("batch_id:", batch_id)
    print("ingested_at:", ingested_at)

    insert_fn(df, source_file, batch_id, ingested_at)
    print(f"{dataset_name} load completed successfully.\n")


def main():
    print("Starting MinIO -> Postgres full raw load...")

    process_dataset("users", insert_users)
    process_dataset("merchants", insert_merchants)
    process_dataset("payments", insert_payments)
    process_dataset("product_events", insert_product_events)

    print("All dataset loads completed successfully.")


if __name__ == "__main__":
    main()
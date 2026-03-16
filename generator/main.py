import os
from datetime import date
import boto3
from users import generate_users
from merchants import generate_merchants
from payments import generate_payments
from events import generate_product_events

RUN_DATE = str(date.today())

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "fintech-wiorx-lake")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ROOT_USER,
    aws_secret_access_key=MINIO_ROOT_PASSWORD,
)

def ensure_bucket_exists():
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Created bucket: {MINIO_BUCKET}")

def save_and_upload(df, local_path, bucket_key):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    df.to_parquet(local_path, index=False)
    s3.upload_file(local_path, MINIO_BUCKET, bucket_key)
    print(f"Uploaded: s3://{MINIO_BUCKET}/{bucket_key}")

def main():
    ensure_bucket_exists()
    users_df = generate_users(RUN_DATE, 200)
    merchants_df = generate_merchants(RUN_DATE, 50)
    payments_df = generate_payments(RUN_DATE, users_df, merchants_df, 4000)
    events_df = generate_product_events(RUN_DATE, payments_df)

    datasets = [
        ("users", users_df),
        ("merchants", merchants_df),
        ("payments", payments_df),
        ("product_events", events_df),
    ]

    for name, df in datasets:
        local_path = f"/app/data/raw/{name}/date={RUN_DATE}/{name}.parquet"
        bucket_key = f"raw/{name}/date={RUN_DATE}/{name}.parquet"
        save_and_upload(df, local_path, bucket_key)

if __name__ == "__main__":
    main()
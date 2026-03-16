import os
from datetime import datetime, timezone
import boto3
from users import generate_users
from merchants import generate_merchants
from payments import generate_payments
from events import generate_product_events

# RUN_BATCH is a timestamp string passed by the caller (e.g. GitHub Actions).
# Each 10-min run writes to its own unique S3 prefix so batches never overwrite.
# Format: YYYYMMDD_HHMMSS  e.g. 20260316_103000
NOW       = datetime.now(timezone.utc)
RUN_BATCH = os.getenv("RUN_BATCH", NOW.strftime("%Y%m%d_%H%M%S"))
RUN_DATE  = RUN_BATCH[:8]   # YYYYMMDD

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT",    "http://minio:9000")
MINIO_ROOT_USER   = os.getenv("MINIO_ROOT_USER",   "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET",      "fintech-wiorx-lake")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ROOT_USER,
    aws_secret_access_key=MINIO_ROOT_PASSWORD,
)

def save_and_upload(df, bucket_key):
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=MINIO_BUCKET, Key=bucket_key, Body=buf.read())
    print(f"Uploaded: s3://{MINIO_BUCKET}/{bucket_key}")

def main():
    print(f"Batch: {RUN_BATCH}")

    # Incremental sizes — small batches every 10 min
    users_df    = generate_users(RUN_DATE, 20)
    merchants_df = generate_merchants(RUN_DATE, 2)
    payments_df = generate_payments(RUN_DATE, users_df, merchants_df, 200)
    events_df   = generate_product_events(RUN_DATE, payments_df)

    datasets = [
        ("users",          users_df),
        ("merchants",      merchants_df),
        ("payments",       payments_df),
        ("product_events", events_df),
    ]

    for name, df in datasets:
        # Each batch gets its own unique S3 path — no overwrites
        bucket_key = f"raw/{name}/date={RUN_DATE}/batch={RUN_BATCH}/{name}.parquet"
        save_and_upload(df, bucket_key)

if __name__ == "__main__":
    main()
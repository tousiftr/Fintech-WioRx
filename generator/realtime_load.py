"""
realtime_load.py
Generates a small realistic batch of fintech data with current timestamps
and inserts it directly into Postgres. Designed to run every 10 minutes.

Per run: ~10-20 new users, ~80-150 new payments, 3 events per payment.
"""
import os
import uuid
import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from sqlalchemy import create_engine, text

fake = Faker()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fintech:fintech123@postgres:5432/fintech")
engine = create_engine(DATABASE_URL)

COUNTRIES  = ["GB", "DE", "FR", "NL", "ES"]
CURRENCIES = {"GB": "GBP", "DE": "EUR", "FR": "EUR", "NL": "EUR", "ES": "EUR"}


def rand_ts(now):
    """Random timestamp within the last 10 minutes."""
    return now - timedelta(seconds=random.randint(0, 600))


# ── generators ────────────────────────────────────────────────────────────────

def make_users(n, now):
    users = []
    for _ in range(n):
        country = random.choice(COUNTRIES)
        users.append({
            "user_id":             f"usr_{uuid.uuid4().hex[:12]}",
            "created_at":          rand_ts(now),
            "country":             country,
            "acquisition_channel": random.choice(["organic", "paid_search", "referral", "affiliate"]),
            "kyc_status":          random.choices(["approved", "pending", "rejected"], weights=[0.8, 0.15, 0.05])[0],
            "device_type":         random.choice(["ios", "android", "web"]),
        })
    return users


def maybe_make_merchant(now):
    """1-in-5 chance of a new merchant joining this run."""
    if random.random() > 0.2:
        return []
    return [{
        "merchant_id":   f"mrc_{uuid.uuid4().hex[:12]}",
        "merchant_name": fake.company(),
        "category":      random.choice(["food", "retail", "travel", "entertainment", "health"]),
        "country":       random.choice(COUNTRIES),
        "created_at":    rand_ts(now),
    }]


def fetch_existing_merchant_ids():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT merchant_id FROM raw.merchants ORDER BY random() LIMIT 100"))
        return [r[0] for r in rows]


def make_payments(users, all_merchant_ids, n, now):
    payments, events = [], []
    for _ in range(n):
        user       = random.choice(users)
        country    = user["country"]
        status     = random.choices(["succeeded", "failed"], weights=[0.88, 0.12])[0]
        ts         = rand_ts(now)
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        pay_id     = f"pay_{uuid.uuid4().hex[:12]}"

        payments.append({
            "payment_id":     pay_id,
            "user_id":        user["user_id"],
            "merchant_id":    random.choice(all_merchant_ids),
            "amount":         round(random.uniform(5, 500), 2),
            "currency":       CURRENCIES.get(country, "EUR"),
            "payment_method": random.choice(["card", "bank_transfer", "wallet"]),
            "gateway":        random.choice(["stripe", "adyen", "checkout"]),
            "status":         status,
            "created_at":     ts,
        })

        final_event = "payment_succeeded" if status == "succeeded" else "payment_failed"
        for event_name in ["checkout_opened", "payment_submitted", final_event]:
            events.append({
                "event_id":        f"evt_{uuid.uuid4().hex[:12]}",
                "event_name":      event_name,
                "event_timestamp": ts,
                "user_id":         user["user_id"],
                "payment_id":      pay_id,
                "session_id":      session_id,
                "platform":        random.choice(["ios", "android", "web"]),
                "country":         country,
            })

    return payments, events


# ── inserters ─────────────────────────────────────────────────────────────────

def insert_users(rows, now, batch_id):
    if not rows:
        return
    data = [{**r, "source_file": "realtime", "ingested_at": now, "batch_id": batch_id} for r in rows]
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw.users
                (user_id, created_at, country, acquisition_channel, kyc_status, device_type,
                 source_file, ingested_at, batch_id)
            VALUES
                (:user_id, :created_at, :country, :acquisition_channel, :kyc_status, :device_type,
                 :source_file, :ingested_at, :batch_id)
            ON CONFLICT (user_id) DO NOTHING
        """), data)
    print(f"  users:     +{len(data)}")


def insert_merchants(rows, now, batch_id):
    if not rows:
        return
    data = [{**r, "source_file": "realtime", "ingested_at": now, "batch_id": batch_id} for r in rows]
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw.merchants
                (merchant_id, merchant_name, category, country, created_at,
                 source_file, ingested_at, batch_id)
            VALUES
                (:merchant_id, :merchant_name, :category, :country, :created_at,
                 :source_file, :ingested_at, :batch_id)
            ON CONFLICT (merchant_id) DO NOTHING
        """), data)
    print(f"  merchants: +{len(data)}")


def insert_payments(rows, now, batch_id):
    if not rows:
        return
    data = [{**r, "source_file": "realtime", "ingested_at": now, "batch_id": batch_id} for r in rows]
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw.payments
                (payment_id, user_id, merchant_id, amount, currency, payment_method,
                 gateway, status, created_at, source_file, ingested_at, batch_id)
            VALUES
                (:payment_id, :user_id, :merchant_id, :amount, :currency, :payment_method,
                 :gateway, :status, :created_at, :source_file, :ingested_at, :batch_id)
            ON CONFLICT (payment_id) DO NOTHING
        """), data)
    print(f"  payments:  +{len(data)}")


def insert_events(rows, now, batch_id):
    if not rows:
        return
    data = [{**r, "source_file": "realtime", "ingested_at": now, "batch_id": batch_id} for r in rows]
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw.product_events
                (event_id, event_name, event_timestamp, user_id, payment_id,
                 session_id, platform, country, source_file, ingested_at, batch_id)
            VALUES
                (:event_id, :event_name, :event_timestamp, :user_id, :payment_id,
                 :session_id, :platform, :country, :source_file, :ingested_at, :batch_id)
            ON CONFLICT (event_id) DO NOTHING
        """), data)
    print(f"  events:    +{len(data)}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    now      = datetime.now(timezone.utc)
    batch_id = f"rt_{now.strftime('%Y%m%d%H%M%S')}"

    print(f"[{now.isoformat()}] Starting realtime batch {batch_id}")

    users     = make_users(random.randint(10, 20), now)
    merchants = maybe_make_merchant(now)

    existing_merchant_ids = fetch_existing_merchant_ids()
    new_merchant_ids      = [m["merchant_id"] for m in merchants]
    all_merchant_ids      = existing_merchant_ids + new_merchant_ids

    if not all_merchant_ids:
        print("  No merchants in DB yet — skipping payments. Run main.py first to seed merchants.")
        insert_users(users, now, batch_id)
        return

    payments, events = make_payments(users, all_merchant_ids, random.randint(80, 150), now)

    insert_users(users, now, batch_id)
    insert_merchants(merchants, now, batch_id)
    insert_payments(payments, now, batch_id)
    insert_events(events, now, batch_id)

    print(f"Done — {len(users)} users, {len(payments)} payments, {len(events)} events.")


if __name__ == "__main__":
    main()

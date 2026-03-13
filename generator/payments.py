import pandas as pd
import uuid
import random
from faker import Faker

fake = Faker()

def generate_payments(run_date: str, users_df: pd.DataFrame, merchants_df: pd.DataFrame, n_payments: int = 4000) -> pd.DataFrame:
    currencies = {"GB": "GBP", "DE": "EUR", "FR": "EUR", "NL": "EUR", "ES": "EUR"}
    methods = ["card", "bank_transfer", "wallet"]
    gateways = ["stripe", "adyen", "checkout"]
    statuses = ["succeeded", "failed"]

    rows = []
    for _ in range(n_payments):
        user = users_df.sample(1).iloc[0]
        merchant = merchants_df.sample(1).iloc[0]
        country = user["country"]
        status = random.choices(statuses, weights=[0.88, 0.12])[0]

        rows.append({
            "payment_id": f"pay_{uuid.uuid4().hex[:12]}",
            "user_id": user["user_id"],
            "merchant_id": merchant["merchant_id"],
            "amount": round(random.uniform(5, 500), 2),
            "currency": currencies.get(country, "EUR"),
            "payment_method": random.choice(methods),
            "gateway": random.choice(gateways),
            "status": status,
            "created_at": fake.date_time_this_month().isoformat()
        })

    return pd.DataFrame(rows)
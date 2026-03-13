import pandas as pd
import uuid
import random
from faker import Faker

fake = Faker()

def generate_users(run_date: str, n_users: int = 200) -> pd.DataFrame:
    rows = []
    countries = ["GB", "DE", "FR", "NL", "ES"]
    channels = ["organic", "paid_search", "referral", "affiliate"]
    kyc_statuses = ["approved", "pending", "rejected"]
    device_types = ["ios", "android", "web"]

    for _ in range(n_users):
        rows.append({
            "user_id": f"usr_{uuid.uuid4().hex[:12]}",
            "created_at": fake.date_time_this_year().isoformat(),
            "country": random.choice(countries),
            "acquisition_channel": random.choice(channels),
            "kyc_status": random.choices(kyc_statuses, weights=[0.8, 0.15, 0.05])[0],
            "device_type": random.choice(device_types)
        })

    return pd.DataFrame(rows)
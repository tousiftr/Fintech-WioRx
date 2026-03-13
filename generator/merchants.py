import pandas as pd
import uuid
import random
from faker import Faker

fake = Faker()

def generate_merchants(run_date: str, n_merchants: int = 50) -> pd.DataFrame:
    categories = ["ecommerce", "travel", "food_delivery", "saas", "gaming"]
    countries = ["GB", "DE", "FR", "NL", "ES"]

    rows = []
    for _ in range(n_merchants):
        rows.append({
            "merchant_id": f"mrc_{uuid.uuid4().hex[:10]}",
            "merchant_name": fake.company(),
            "category": random.choice(categories),
            "country": random.choice(countries),
            "created_at": fake.date_time_this_year().isoformat()
        })

    return pd.DataFrame(rows)
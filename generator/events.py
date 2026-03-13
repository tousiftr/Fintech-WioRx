import pandas as pd
import uuid
import random

def generate_product_events(run_date: str, payments_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, p in payments_df.iterrows():
        session_id = f"sess_{uuid.uuid4().hex[:12]}"

        rows.append({
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "event_name": "checkout_opened",
            "event_timestamp": p["created_at"],
            "user_id": p["user_id"],
            "payment_id": p["payment_id"],
            "session_id": session_id,
            "platform": random.choice(["ios", "android", "web"]),
            "country": None
        })

        rows.append({
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "event_name": "payment_submitted",
            "event_timestamp": p["created_at"],
            "user_id": p["user_id"],
            "payment_id": p["payment_id"],
            "session_id": session_id,
            "platform": random.choice(["ios", "android", "web"]),
            "country": None
        })

        final_event = "payment_succeeded" if p["status"] == "succeeded" else "payment_failed"
        rows.append({
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "event_name": final_event,
            "event_timestamp": p["created_at"],
            "user_id": p["user_id"],
            "payment_id": p["payment_id"],
            "session_id": session_id,
            "platform": random.choice(["ios", "android", "web"]),
            "country": None
        })

    return pd.DataFrame(rows)
"""
Generates mock raw data files simulating a daily extract from an upstream
e-commerce / audience-measurement source system (e.g. a vendor API or S3 drop).

Run standalone for local testing:
    python scripts/generate_mock_data.py

In the Airflow DAG, this same logic is called by the `extract_raw_data` task
to simulate pulling a new daily batch — mirroring a real "ingest external
vendor data -> land in raw zone -> transform" pattern.
"""

import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

REGIONS = ["US-West", "US-East", "US-Central", "EU", "APAC"]
DEVICE_TYPES = ["desktop", "mobile", "tablet", "ctv"]
CATEGORIES = ["electronics", "apparel", "home", "beauty", "sports"]


def generate_customers(n=500):
    path = os.path.join(RAW_DIR, "customers.csv")
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "signup_date", "region", "is_member"])
        start = datetime(2023, 1, 1)
        for i in range(1, n + 1):
            signup = start + timedelta(days=random.randint(0, 900))
            writer.writerow(
                [i, signup.strftime("%Y-%m-%d"), random.choice(REGIONS), random.choice([0, 1])]
            )
    return path


def generate_products(n=80):
    path = os.path.join(RAW_DIR, "products.csv")
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "category", "unit_price"])
        for i in range(1, n + 1):
            writer.writerow([i, random.choice(CATEGORIES), round(random.uniform(9.99, 299.99), 2)])
    return path


def generate_orders(n=4000, customer_count=500, product_count=80):
    path = os.path.join(RAW_DIR, "orders.csv")
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["order_id", "customer_id", "product_id", "order_ts", "quantity", "device_type", "status"]
        )
        start = datetime(2024, 1, 1)
        # inject a small number of intentional data-quality issues so dbt
        # tests have something real to catch (mirrors real vendor-feed noise)
        bad_row_ids = set(random.sample(range(1, n + 1), k=8))
        for i in range(1, n + 1):
            ts = start + timedelta(minutes=random.randint(0, 60 * 24 * 500))
            customer_id = random.randint(1, customer_count)
            product_id = random.randint(1, product_count)
            qty = random.randint(1, 5)
            status = random.choices(
                ["completed", "cancelled", "refunded"], weights=[0.88, 0.08, 0.04]
            )[0]
            if i in bad_row_ids:
                # simulate a null FK / orphan record from an upstream feed glitch
                customer_id = ""
            writer.writerow(
                [i, customer_id, product_id, ts.strftime("%Y-%m-%d %H:%M:%S"), qty,
                 random.choice(DEVICE_TYPES), status]
            )
    return path


def run():
    c = generate_customers()
    p = generate_products()
    o = generate_orders()
    print(f"Wrote: {c}\nWrote: {p}\nWrote: {o}")


if __name__ == "__main__":
    run()

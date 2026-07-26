"""
generate_fake_data.py

Generates fake e-commerce data for the sales pipeline project:
  - customers.csv
  - products.csv
  - orders.json  (partitioned by date, simulating daily "new orders" landing)

Deliberately injects data quality issues so there's real cleaning work to do
in the Databricks silver layer later:
  - duplicate order_ids
  - null / missing fields
  - inconsistent date formats
  - a few negative / zero amounts
  - inconsistent casing / whitespace in text fields

Usage:
    python generate_fake_data.py --days 10 --orders-per-day 50
"""

import argparse
import json
import os
import random
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

REGIONS = ["East", "West", "North", "South"]
STATUSES = ["pending", "shipped", "delivered", "cancelled"]
CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Toys"]

DATE_FORMATS = [
    "%Y-%m-%d",        # 2026-01-25
    "%d/%m/%Y",        # 25/01/2026
    "%m-%d-%Y",        # 01-25-2026
    "%Y/%m/%d %H:%M",  # 2026/01/25 14:30
]


def random_date_format(dt: datetime) -> str:
    """Return the date formatted in a randomly chosen (messy) format."""
    fmt = random.choice(DATE_FORMATS)
    return dt.strftime(fmt)


def maybe_messy_text(value: str) -> str:
    """Randomly mess up casing / whitespace on a text field."""
    roll = random.random()
    if roll < 0.1:
        return f"  {value.upper()}  "
    if roll < 0.2:
        return value.lower()
    return value


def generate_customers(n=200):
    customers = []
    for i in range(1, n + 1):
        customer = {
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email() if random.random() > 0.03 else None,  # ~3% missing emails
            "region": maybe_messy_text(random.choice(REGIONS)),
            "signup_date": random_date_format(
                fake.date_time_between(start_date="-3y", end_date="-1M")
            ),
        }
        customers.append(customer)
    return customers


def generate_products(n=50):
    products = []
    for i in range(1, n + 1):
        products.append(
            {
                "product_id": i,
                "product_name": fake.catch_phrase(),
                "category": maybe_messy_text(random.choice(CATEGORIES)),
                "unit_price": round(random.uniform(5, 500), 2),
            }
        )
    return products


def generate_orders(days: int, orders_per_day: int, num_customers: int, num_products: int):
    """
    Generates one list of order dicts per day (simulating daily batches landing
    in the bronze layer). Returns a dict: {date_str: [orders...]}.
    """
    start_date = datetime.now() - timedelta(days=days)
    daily_orders = {}
    next_order_id = 1

    # keep a pool of "already used" order ids so we can reinject a few duplicates
    used_order_ids = []

    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        date_key = current_date.strftime("%Y-%m-%d")
        orders_today = []

        for _ in range(orders_per_day):
            # ~2% chance: duplicate an existing order_id instead of a new one
            if used_order_ids and random.random() < 0.02:
                order_id = random.choice(used_order_ids)
            else:
                order_id = next_order_id
                used_order_ids.append(order_id)
                next_order_id += 1

            amount = round(random.uniform(10, 800), 2)
            # ~3% chance: inject a bad amount (negative or zero)
            if random.random() < 0.03:
                amount = round(random.uniform(-50, 0), 2)

            order = {
                "order_id": order_id,
                "customer_id": random.randint(1, num_customers) if random.random() > 0.02 else None,
                "product_id": random.randint(1, num_products),
                "quantity": random.randint(1, 5),
                "amount": amount,
                "status": maybe_messy_text(random.choice(STATUSES)),
                "order_date": random_date_format(
                    current_date + timedelta(
                        hours=random.randint(0, 23), minutes=random.randint(0, 59)
                    )
                ),
            }
            orders_today.append(order)

        daily_orders[date_key] = orders_today

    return daily_orders


def write_csv(rows, path, fieldnames):
    import csv

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {path}")


def write_json(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    print(f"Wrote {len(rows)} rows -> {path}")


def main():
    parser = argparse.ArgumentParser(description="Generate fake e-commerce data.")
    parser.add_argument("--days", type=int, default=10, help="Number of days of orders to simulate")
    parser.add_argument("--orders-per-day", type=int, default=50, help="Orders generated per day")
    parser.add_argument("--customers", type=int, default=200, help="Number of customers to generate")
    parser.add_argument("--products", type=int, default=50, help="Number of products to generate")
    args = parser.parse_args()

    customers = generate_customers(args.customers)
    products = generate_products(args.products)
    daily_orders = generate_orders(args.days, args.orders_per_day, args.customers, args.products)

    write_csv(
        customers,
        os.path.join(OUTPUT_DIR, "customers", "customers.csv"),
        fieldnames=["customer_id", "name", "email", "region", "signup_date"],
    )
    write_csv(
        products,
        os.path.join(OUTPUT_DIR, "products", "products.csv"),
        fieldnames=["product_id", "product_name", "category", "unit_price"],
    )

    for date_key, orders in daily_orders.items():
        write_json(orders, os.path.join(OUTPUT_DIR, "orders", date_key, "orders.json"))

    # Also write one combined file with every order across all days -
    # convenient for uploading a single file into Databricks (instead of
    # one file per day). Each order still carries its own order_date field,
    # so the daily partitioning information isn't lost.
    all_orders = [order for orders in daily_orders.values() for order in orders]
    write_json(all_orders, os.path.join(OUTPUT_DIR, "orders", "all_orders.json"))

    total_orders = sum(len(v) for v in daily_orders.values())
    print(f"\nDone. Generated {len(customers)} customers, {len(products)} products, "
          f"{total_orders} orders across {args.days} day(s).")
    print(f"Output root: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()
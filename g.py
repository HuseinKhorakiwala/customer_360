import pandas as pd
import random
import json
import uuid
from datetime import datetime, timedelta
import os

# Create output directory
os.makedirs("phase1_data", exist_ok=True)

# Generate more customers
num_customers = 1000
customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, num_customers + 1)]

# ---------------------------
# 1. Transactions CSV
# ---------------------------
transactions = []
for cust_id in customer_ids:
    for _ in range(random.randint(5, 15)):
        txn = {
            "customer_id": cust_id,
            "txn_id": str(uuid.uuid4())[:8],
            "amount": round(random.uniform(100, 10000), 2),
            "type": random.choice(["debit", "credit"]),
            "date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
        }
        transactions.append(txn)

print(f"Generated {len(transactions)} transactions...")
pd.DataFrame(transactions).to_csv("phase1_data/transactions.csv", index=False)


# ---------------------------
# 2. Credit Card JSON
# ---------------------------
credit_cards = []
for cust_id in customer_ids:
    card = {
        "customer_id": cust_id,
        "card_no": str(random.randint(4000000000000000, 4999999999999999)),
        "limit": random.choice([50000, 100000, 200000]),
        "usage": round(random.uniform(0.05, 0.99), 2),
        "due_date": (datetime.now() + timedelta(days=random.randint(1, 30))).strftime("%d-%m-%Y")
    }
    credit_cards.append(card)

print(f"Generated {len(credit_cards)} credit card records...")
with open("phase1_data/credit_card.json", "w") as f:
    json.dump(credit_cards, f, indent=2)


# ---------------------------
# 3. App Logs JSON (Nested JSON)
# ---------------------------
app_logs = []
for cust_id in customer_ids:
    for _ in range(random.randint(2, 6)):
        session = {
            "session_id": str(uuid.uuid4())[:6],
            "customer_id": cust_id,
            "actions": random.choices(
                ["open_app", "view_product", "search", "add_to_cart", "checkout", "logout"],
                k=random.randint(3, 6)
            ),
            "timestamps": [
                (datetime.now() - timedelta(minutes=random.randint(1, 10000))).isoformat()
                for _ in range(3)
            ]
        }
        app_logs.append(session)

print(f"Generated {len(app_logs)} app log sessions...")
with open("phase1_data/app_logs.json", "w") as f:
    json.dump(app_logs, f, indent=2)


# ---------------------------
# 4. Support Tickets CSV
# ---------------------------
support_tickets = []
for cust_id in customer_ids:
    for _ in range(random.randint(0, 4)):
        ticket = {
            "ticket_id": str(uuid.uuid4())[:8],
            "customer_id": cust_id,
            "issue_type": random.choice(["billing", "login", "technical", "delivery", "refund", "other"]),
            "resolution_time": round(random.uniform(0.1, 120), 2)  # in hours
        }
        support_tickets.append(ticket)

print(f"Generated {len(support_tickets)} support tickets...")
pd.DataFrame(support_tickets).to_csv("phase1_data/support.csv", index=False)

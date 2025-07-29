import pandas as pd

# Load all cleaned data
transactions = pd.read_csv("phase3_cleaned/transactions_cleaned.csv")
support = pd.read_csv("phase3_cleaned/support_cleaned.csv")
credit_card = pd.read_csv("phase3_cleaned/credit_card_cleaned.csv")
app_logs = pd.read_csv("phase3_cleaned/app_logs_cleaned.csv")

# 📊 Transaction Features
txn_agg = transactions.groupby("customer_id").agg({
    "amount": ["sum", "mean"],
    "type": lambda x: (x == "debit").sum(),  # debit count
})
txn_agg.columns = ["total_amount", "avg_amount", "debit_count"]
txn_agg = txn_agg.reset_index()

# Add credit count separately
credit_count = transactions[transactions["type"] == "credit"].groupby("customer_id").size().reset_index(name="credit_count")
txn_agg = txn_agg.merge(credit_count, on="customer_id", how="left")
txn_agg["credit_count"] = txn_agg["credit_count"].fillna(0)

# 🛠️ Support Features
support_agg = support.groupby("customer_id").agg({
    "ticket_id": "count",
    "resolution_time": "mean"
}).rename(columns={"ticket_id": "ticket_count", "resolution_time": "avg_resolution_time"}).reset_index()

# 💳 Credit Card Data (already one row per customer)
credit_card["usage_percent"] = credit_card["usage"] * 100

# 📱 App Logs Features
logs_agg = app_logs.groupby("customer_id").agg({
    "session_id": pd.Series.nunique,
    "action": pd.Series.nunique,
    "timestamp": "max"
}).rename(columns={
    "session_id": "unique_sessions",
    "action": "unique_actions",
    "timestamp": "last_active"
}).reset_index()

# 🧩 Merge all into one Customer 360 DataFrame
cust360 = txn_agg.merge(support_agg, on="customer_id", how="outer")
cust360 = cust360.merge(credit_card[["customer_id", "limit", "usage_percent", "due_date"]], on="customer_id", how="outer")
cust360 = cust360.merge(logs_agg, on="customer_id", how="outer")

# 💾 Save to CSV
cust360.to_csv("customer_360.csv", index=False)
print("✅ Aggregated Customer 360 Profile saved to customer_360.csv")

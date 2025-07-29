import pandas as pd

df = pd.read_csv("customer_360.csv")

print("📊 Top 5 customers by total amount spent:")
print(df.sort_values("total_amount", ascending=False)[["customer_id", "total_amount"]].head())

print("\n🔍 Customers with high card usage (>80%):")
print(df[df["usage_percent"] > 80][["customer_id", "usage_percent"]])

print("\n📉 Customers with most support issues:")
print(df.sort_values("ticket_count", ascending=False)[["customer_id", "ticket_count"]].head())

import pandas as pd

# Load cleaned CSV files from the local folder
transactions_df = pd.read_csv("phase3_cleaned/transactions_cleaned.csv")
support_df = pd.read_csv("phase3_cleaned/support_cleaned.csv")
credit_df = pd.read_csv("phase3_cleaned/credit_card_cleaned.csv")
logs_df = pd.read_csv("phase3_cleaned/app_logs_cleaned.csv")

# Print column names and preview each DataFrame (optional)
print("📄 Transactions:\n", transactions_df.columns, "\n", transactions_df.head(), "\n")
print("📄 Support:\n", support_df.columns, "\n", support_df.head(), "\n")
print("📄 Credit Card:\n", credit_df.columns, "\n", credit_df.head(), "\n")
print("📄 App Logs:\n", logs_df.columns, "\n", logs_df.head(), "\n")


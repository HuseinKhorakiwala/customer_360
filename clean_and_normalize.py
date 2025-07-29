import boto3
import pandas as pd
import os
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, REGION, BUCKET_NAME

# Set up boto3 client
s3 = boto3.client(
    's3',
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

RAW_KEYS = {
    'transactions': 'raw/transactions/transactions.csv',
    'support': 'raw/support/support.csv',
    'credit_card': 'raw/credit_card/credit_card.json',
    'app_logs': 'raw/app_logs/app_logs.json'
}

os.makedirs("phase3_cleaned", exist_ok=True)

def download_from_s3(key, local_path):
    s3.download_file(BUCKET_NAME, key, local_path)
    print(f"✅ Downloaded: {key}")

def upload_to_s3(local_path, key):
    s3.upload_file(local_path, BUCKET_NAME, key)
    print(f"⬆️ Uploaded to S3: {key}")

# ------------------ Cleaners ------------------
def clean_transactions(df):
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df.dropna()
    return df

def clean_support(df):
    # Convert resolution_time to numeric (e.g., minutes/hours), if needed
    if 'resolution_time' in df.columns:
        df['resolution_time'] = pd.to_numeric(df['resolution_time'], errors='coerce')

    # Drop rows with missing critical info
    df.dropna(subset=['ticket_id', 'customer_id'], inplace=True)

    return df


def clean_credit_card(df):
    df['limit'] = pd.to_numeric(df['limit'], errors='coerce')
    #df['valid_from'] = pd.to_datetime(df['valid_from'], errors='coerce')
    return df.dropna()

def clean_app_logs(df):
    # Step 1: Ensure lists are valid
    df['timestamps'] = df['timestamps'].apply(lambda x: x if isinstance(x, list) else [])
    df['actions'] = df['actions'].apply(lambda x: x if isinstance(x, list) else [])

    # Step 2: Filter only those rows where lengths match
    df = df[df['timestamps'].str.len() == df['actions'].str.len()]

    # Step 3: Explode both columns
    df = df.explode(['timestamps', 'actions'])

    # Step 4: Rename for clarity
    df.rename(columns={'timestamps': 'timestamp', 'actions': 'action'}, inplace=True)

    # Step 5: Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    return df



# ------------------ Execution ------------------
for name, key in RAW_KEYS.items():
    local_raw = f"{name}.csv" if key.endswith(".csv") else f"{name}.json"
    local_cleaned = f"phase3_cleaned/{name}_cleaned.csv"
    s3_cleaned_key = f"cleaned/{name}_cleaned.csv"

    # Download raw file
    download_from_s3(key, local_raw)

    # Load and clean
    if key.endswith(".csv"):
        df = pd.read_csv(local_raw)
    else:
        df = pd.read_json(local_raw)

    if name == 'transactions':
        cleaned_df = clean_transactions(df)
    elif name == 'support':
        cleaned_df = clean_support(df)
    elif name == 'credit_card':
        cleaned_df = clean_credit_card(df)
    elif name == 'app_logs':
        cleaned_df = clean_app_logs(df)

    # Save cleaned file locally
    cleaned_df.to_csv(local_cleaned, index=False)

    # Upload cleaned file to S3
    upload_to_s3(local_cleaned, s3_cleaned_key)

    print(f"✅ Done processing: {name}\n")


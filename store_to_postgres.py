# store_to_postgres.py
import pandas as pd
from sqlalchemy import create_engine
import config  # your DB credentials

# Load final customer dataset
df = pd.read_csv("customer_360.csv")

# Create connection string
engine = create_engine(
    f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
)

# Store into PostgreSQL table
df.to_sql("customer_360", engine, if_exists="replace", index=False)

print("✅ customer_360 data stored successfully in PostgreSQL.")

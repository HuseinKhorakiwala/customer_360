import streamlit as st
import pandas as pd

# Load data
df = pd.read_csv("customer_360.csv")

st.title("📊 Customer 360 Dashboard")

# Total Customers
st.metric("Total Customers", len(df))

# Filter by customer
customer_id = st.selectbox("Select Customer", df["customer_id"].unique())

customer_data = df[df["customer_id"] == customer_id]

st.subheader(f"📋 Details for {customer_id}")
st.write(customer_data.T)

# Charts
st.subheader("📈 Usage Patterns")
st.bar_chart(df.set_index("customer_id")[["total_amount", "debit_count", "credit_count"]].head(10))

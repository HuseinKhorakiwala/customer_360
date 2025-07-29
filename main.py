from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from typing import List, Optional

app = FastAPI(title="Customer 360 API")

# ✅ PostgreSQL connection details
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="cust360_db",
    user="postgres",
    password=""  # replace with your actual password
)
cursor = conn.cursor()

# ✅ Pydantic model
class Customer(BaseModel):
    customer_id: int
    name: str
    email: str
    phone: Optional[str]
    address: Optional[str]
    account_balance: Optional[float]

# ✅ GET all customers
@app.get("/customers", response_model=List[Customer])
def get_customers():
    cursor.execute("SELECT * FROM unified_customers;")
    rows = cursor.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No customers found.")
    return [Customer(**dict(zip([desc[0] for desc in cursor.description], row))) for row in rows]

# ✅ GET customer by ID
@app.get("/customers/{customer_id}", response_model=Customer)
def get_customer(customer_id: int):
    cursor.execute("SELECT * FROM unified_customers WHERE customer_id = %s;", (customer_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Customer not found.")
    return Customer(**dict(zip([desc[0] for desc in cursor.description], row)))

# ✅ Search by name
@app.get("/customers/search/", response_model=List[Customer])
def search_customer(name: str):
    cursor.execute("SELECT * FROM unified_customers WHERE LOWER(name) LIKE %s;", (f"%{name.lower()}%",))
    rows = cursor.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No matching customers.")
    return [Customer(**dict(zip([desc[0] for desc in cursor.description], row))) for row in rows]

# ✅ Add a new customer
@app.post("/customers/add", response_model=Customer)
def add_customer(cust: Customer):
    cursor.execute(
        """
        INSERT INTO unified_customers (customer_id, name, email, phone, address, account_balance)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (cust.customer_id, cust.name, cust.email, cust.phone, cust.address, cust.account_balance)
    )
    conn.commit()
    return cust

# ✅ Update customer
@app.put("/customers/update/{customer_id}", response_model=Customer)
def update_customer(customer_id: int, cust: Customer):
    cursor.execute("SELECT * FROM unified_customers WHERE customer_id = %s;", (customer_id,))
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="Customer not found.")

    cursor.execute(
        """
        UPDATE unified_customers
        SET name = %s, email = %s, phone = %s, address = %s, account_balance = %s
        WHERE customer_id = %s;
        """,
        (cust.name, cust.email, cust.phone, cust.address, cust.account_balance, customer_id)
    )
    conn.commit()
    return cust

# ✅ Delete customer
@app.delete("/customers/delete/{customer_id}")
def delete_customer(customer_id: int):
    cursor.execute("SELECT * FROM unified_customers WHERE customer_id = %s;", (customer_id,))
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="Customer not found.")
    
    cursor.execute("DELETE FROM unified_customers WHERE customer_id = %s;", (customer_id,))
    conn.commit()
    return {"detail": f"Customer {customer_id} deleted."}

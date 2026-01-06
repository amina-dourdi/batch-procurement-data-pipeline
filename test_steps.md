# Procurement Pipeline: Data Generation & Database Setup

This guide explains how to generate realistic test data using Python (Faker).

Here is the fully organized guide with clear titles, structuring the entire workflow from Python generation to PostgreSQL deployment.

---
### PostgreSQL Dataset Generation 

## 1. Project Structure

First, ensure your project folder looks exactly like this. Create the folders if they don't exist.

```text
/procurement_project/
│
├── data/
│   ├── postgres_load/       # (Empty folder - CSVs will be generated here)
│   └── hdfs/                # (Empty folder - Parquet files will go here later)
│
├── scripts/
│   └── generate_master_data.py   # The Python Generator Code
│
├── sql/
│   └── init_schema.sql      # The SQL Reference (Backup)
│
└── setup_db.sh              # The Automation Script (Master Controller)

```

---

## 2. The Python Generator

Create the file `scripts/generate_master_data.py` and paste this code. This script generates 3 CSV files: `suppliers.csv`, `market.csv`, and `products.csv` (with procurement rules embedded).

```python
import pandas as pd
import random
from faker import Faker
import os

# Initialize Faker
fake = Faker()
Faker.seed(42)  # Ensures the data is the same every time we run it

# --- CONFIGURATION ---
NUM_SUPPLIERS = 15
NUM_MARKETS = 10
NUM_PRODUCTS = 100
OUTPUT_DIR = "data/postgres_load"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"🚀 Starting Data Generation in '{OUTPUT_DIR}'...")

# ==========================================
# 1. GENERATE SUPPLIERS
# ==========================================
print("--- Generating Suppliers ---")
suppliers = []
supplier_ids = [f"SUP-{str(i).zfill(3)}" for i in range(1, NUM_SUPPLIERS + 1)]

for s_id in supplier_ids:
    suppliers.append({
        "supplier_id": s_id,
        "name": fake.company(),
        "country": fake.country(),
        "contact_email": fake.company_email(),
        "location": fake.city()
    })

df_suppliers = pd.DataFrame(suppliers)
df_suppliers.to_csv(f"{OUTPUT_DIR}/suppliers.csv", index=False)
print(f"✔ Created {len(df_suppliers)} suppliers.")

# ==========================================
# 2. GENERATE MARKETS (Points of Sale)
# ==========================================
print("--- Generating Markets ---")
markets = []
market_ids = [f"MKT-{str(i).zfill(3)}" for i in range(1, NUM_MARKETS + 1)]

for m_id in market_ids:
    markets.append({
        "market_id": m_id,
        "location": fake.address().replace("\n", ", "),
        "type": random.choice(["Superstore", "Express", "Click & Collect"])
    })

df_markets = pd.DataFrame(markets)
df_markets.to_csv(f"{OUTPUT_DIR}/market.csv", index=False)
print(f"✔ Created {len(df_markets)} markets.")

# ==========================================
# 3. GENERATE PRODUCTS (With Rules)
# ==========================================
print("--- Generating Products ---")
products = []
categories = ['Dairy', 'Bakery', 'Canned Goods', 'Beverages', 'Cleaning', 'Produce', 'Meat']
package_types = ['Box of 6', 'Box of 12', 'Box of 24', 'Single Unit', 'Pallet']
sku_list = [f"SKU-{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]

for sku in sku_list:
    # Assign a random supplier from the list we just created
    assigned_supplier = random.choice(supplier_ids)
    
    # Logic: MxOQ is always larger than MOQ
    moq = random.choice([10, 50, 100])
    mxoq = moq * random.randint(5, 20) 
    
    products.append({
        "sku": sku,
        "name": fake.commerce_product_name(),
        "category": random.choice(categories),
        "unit_price": round(random.uniform(2.50, 150.00), 2),
        "supplier_id": assigned_supplier,
        "moq": moq,
        "mxoq": mxoq,
        "package": random.choice(package_types),
        "leadtime": random.randint(1, 14)
    })

df_products = pd.DataFrame(products)
df_products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
print(f"✔ Created {len(df_products)} products.")

print("\n✨ Generation Complete.")

```

---

## 3. The SQL Schema (Reference)

Create the file `sql/init_schema.sql`. This file serves as documentation for your database structure.

```sql
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS market CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;

-- 1. Suppliers Table
CREATE TABLE suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    contact_email VARCHAR(100),
    location VARCHAR(100)
);

-- 2. Market Table (Points of Sale)
CREATE TABLE market (
    market_id VARCHAR(20) PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    type VARCHAR(50)
);

-- 3. Products Table (with embedded Procurement Rules)
CREATE TABLE products (
    sku VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    unit_price DECIMAL(10, 2),
    supplier_id VARCHAR(20) REFERENCES suppliers(supplier_id),
    moq INT DEFAULT 1,
    mxoq INT,
    package VARCHAR(50),
    leadtime INT
);

```

---

## 4. The Automation Script (Bash)

Create the file `setup_db.sh` in your root folder. This script automates the entire process: python generation -> docker copy -> SQL execution.

**⚠️ IMPORTANT:** Change `CONTAINER_NAME="postgres"` to the actual name of your container if it is different.

```bash
#!/bin/bash

# Configuration
CONTAINER_NAME="postgres"  # <--- CHECK THIS NAME (e.g., procurement_db_1)
DB_USER="postgres"
DB_NAME="postgres"
CSV_DIR="data/postgres_load"

echo "=========================================="
echo "🚀 [Step 1] Generating fresh data with Python..."
echo "=========================================="
python scripts/generate_master_data.py

echo ""
echo "=========================================="
echo "📦 [Step 2] Copying CSVs to Docker Container..."
echo "=========================================="
# Create a temp folder inside the container
docker exec $CONTAINER_NAME mkdir -p /tmp/data_load

# Copy files
docker cp $CSV_DIR/suppliers.csv $CONTAINER_NAME:/tmp/data_load/suppliers.csv
docker cp $CSV_DIR/market.csv $CONTAINER_NAME:/tmp/data_load/market.csv
docker cp $CSV_DIR/products.csv $CONTAINER_NAME:/tmp/data_load/products.csv

echo ""
echo "=========================================="
echo "🛠️ [Step 3] Recreating Tables & Importing Data..."
echo "=========================================="

# Pass SQL commands directly to the psql tool inside the container
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME <<EOF

-- A. DROP AND RECREATE TABLES
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS market CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;

CREATE TABLE suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50),
    contact_email VARCHAR(100),
    location VARCHAR(100)
);

CREATE TABLE market (
    market_id VARCHAR(20) PRIMARY KEY,
    location VARCHAR(100),
    type VARCHAR(50)
);

CREATE TABLE products (
    sku VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10, 2),
    supplier_id VARCHAR(20) REFERENCES suppliers(supplier_id),
    moq INT,
    mxoq INT,
    package VARCHAR(50),
    leadtime INT
);

-- B. COPY DATA FROM CSV
COPY suppliers(supplier_id, name, country, contact_email, location) 
FROM '/tmp/data_load/suppliers.csv' DELIMITER ',' CSV HEADER;

COPY market(market_id, location, type) 
FROM '/tmp/data_load/market.csv' DELIMITER ',' CSV HEADER;

COPY products(sku, name, category, unit_price, supplier_id, moq, mxoq, package, leadtime) 
FROM '/tmp/data_load/products.csv' DELIMITER ',' CSV HEADER;

EOF

echo "✅ SUCCESS: Database is ready and fully populated!"

```

---

## 5. How to Run It

Open your terminal (Git Bash or VS Code Terminal) in the `procurement_project` folder.

1. **Make the script executable** (you only need to do this once):
```bash
chmod +x setup_db.sh

```


2. **Run the script**:
```bash
./setup_db.sh

```


3. **Verify the results**:
You can log into your database and check the count:
```bash
docker exec -it postgres psql -U postgres -c "SELECT COUNT(*) FROM products;"

```

### Quick Verification Command

Run this command in your **Git Bash**. It connects to the container and counts the rows in all three tables.

```bash
docker exec -i postgres psql -U procurement_user -d procurement_db -c "
SELECT 'Suppliers' as table, COUNT(*) FROM suppliers UNION ALL
SELECT 'Markets' as table, COUNT(*) FROM market UNION ALL
SELECT 'Products' as table, COUNT(*) FROM products;"

```

**Expected Output:**
If everything worked, you should see numbers matching your Python script (15 suppliers, 10 markets, 100 products):

```text
   table   | count
-----------+-------
 Suppliers |    20
 Markets   |    10
 Products  |   300
(3 rows)

```

If you see this, **Success!** You have no problems.

-----------------------------------------------------------------

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

### PostgreSQL HDFS Generation 

# Procurement Pipeline: Data Generation, PostgreSQL Setup, and HDFS Pipeline (Steps)

This guide shows the full workflow:

1. Generate **master data CSV** with Python (Faker)
2. Load master data into **PostgreSQL** (OLTP layer)
3. Generate **daily RAW Parquet** and store them in **HDFS**
4. Run the batch pipeline to produce **processed** + **supplier orders** in **HDFS**
5. Validate results

> Windows note (IMPORTANT): If you use **Git Bash (MINGW64)**, Linux paths like `/raw/...` may be converted into `C:/...`.
> To avoid `No FileSystem for scheme "C"`, always prefix HDFS CLI commands with:
>
> `MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*"`

---

## 1. Project Structure

Your project should look like:

```text
/batch-procurement-data-pipeline/
│
├── data/
│   ├── postgres_load/        # CSV master data generated here
│   └── tmp/                  # temp local parquet (optional)
│
├── scripts/
│   ├── generate_master_data.py         # (your Faker master generator)
│   ├── pg_client.py                   # Postgres reader
│   ├── hdfs_client.py                 # WebHDFS uploader
│   ├── generate_daily_files_hdfs.py    # Postgres -> RAW Parquet -> HDFS
│   └── run_pipeline_hdfs.py           # HDFS RAW -> processed/output -> HDFS
│
├── sql/
│   ├── 01_schema.sql
│   └── 02_master_data.sql
│
└── docker-compose.yml
```

---

## 2. Generate Master Data CSV (Faker)

Run from project root (host machine):

```bash
python scripts/generate_master_data.py
```

Expected output files:

* `data/postgres_load/suppliers.csv`
* `data/postgres_load/market.csv`
* `data/postgres_load/products.csv`

---

## 3. Load Master Data into PostgreSQL

If your docker-compose already mounts SQL init scripts, you may only need to start containers:

```bash
docker compose up -d
```

### 3.1 Verify PostgreSQL tables and counts

```bash
docker exec -i postgres psql -U procurement_user -d procurement_db -c "
SELECT 'Suppliers' AS table, COUNT(*) FROM suppliers UNION ALL
SELECT 'Markets'   AS table, COUNT(*) FROM market   UNION ALL
SELECT 'Products'  AS table, COUNT(*) FROM products;"
```

### 3.2 Validate FK consistency: products.supplier_id must exist in suppliers

```bash
docker exec -i postgres psql -U procurement_user -d procurement_db -c "
SELECT COUNT(*) AS invalid_products
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.supplier_id
WHERE s.supplier_id IS NULL;"
```

Expected:

* `invalid_products = 0`

---

## 4. Verify HDFS Is Reachable

### 4.1 NameNode UI

Open in browser:

* `http://localhost:9870`

### 4.2 HDFS CLI sanity check (Windows)

> ⚠️ If you use **Git Bash**, use the environment prefix below.

**Option A (recommended): PowerShell / CMD / VS Code terminal**

```powershell
docker exec -it namenode hdfs dfs -ls /
```

**Option B: Git Bash (disable path conversion)**

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /
```

If the output is empty, that can be normal (HDFS root may be empty).

### 4.3 Proof test: create a folder and re-list

**PowerShell/CMD:**

```powershell
docker exec -it namenode hdfs dfs -mkdir -p /test_hdfs
docker exec -it namenode hdfs dfs -ls /
```

**Git Bash:**

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -mkdir -p /test_hdfs
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /
```

Expected:

* you see `/test_hdfs`

---

## 5. Create the HDFS Folder Hierarchy (one time)

Create required directories:

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -mkdir -p /raw/orders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -mkdir -p /raw/stock
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -mkdir -p /processed/aggregated_orders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -mkdir -p /processed/net_demand
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -mkdir -p /output/supplier_orders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -mkdir -p /logs/exceptions
```

Verify:

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /
```

---

## 6. Run Daily RAW Generation (Postgres → Parquet → HDFS)

This script reads master data from PostgreSQL (`products`, `market`, `suppliers`), generates daily RAW Parquet, and uploads to HDFS:

* `/raw/orders/YYYY-MM-DD/orders.parquet`
* `/raw/stock/YYYY-MM-DD/stock.parquet`

Run:

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it orchestrator python /app/scripts/generate_daily_files.py
```

### 6.1 Verify RAW Parquet exists in HDFS

⚠️ `YYYY-MM-DD` is a placeholder. Do NOT type it literally.

1) List the existing date folders first:

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /raw/orders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /raw/stock
```

2) use the real date you see (example: 2026-01-06)

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /raw/orders/2026-01-06
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /raw/stock/2026-01-06
```
---

## 7. Run Pipeline Transformations (HDFS RAW → processed/output → HDFS)

This script downloads RAW Parquet from HDFS, computes:

* aggregated orders
* net demand
* supplier orders (with MOQ + pack rounding)

And uploads results to HDFS:

* `/processed/aggregated_orders/YYYY-MM-DD/aggregated_orders.parquet`
* `/processed/net_demand/YYYY-MM-DD/net_demand.parquet`
* `/output/supplier_orders/YYYY-MM-DD/supplier_orders.parquet`

Run:

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it orchestrator python /app/scripts/run_pipeline_hdfs.py
```

### 7.1 Verify outputs exist in HDFS

⚠️ `YYYY-MM-DD` is a placeholder. Do NOT type it literally.

1) List date folders first (to know the real date folder):

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /processed/aggregated_orders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /processed/net_demand
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /output/supplier_orders
```

2) use the real date you see (example: 2026-01-06)

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /processed/aggregated_orders/2026-01-06
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /processed/net_demand/2026-01-06
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /output/supplier_orders/2026-01-06
```

---

## 8. Validations (what is checked and where)

### 8.1 ID Validations (done inside Python scripts)

* `generate_daily_files.py` ensures:

  * `market_id` comes from Postgres `market` table
  * `sku` comes from Postgres `products` table
  * `products.supplier_id` exists in Postgres `suppliers` table (otherwise the script stops)

* `run_pipeline_hdfs.py` ensures:

  * output `supplier_id` is taken from Postgres `products` for each `sku` (supplier correctness)

### 8.2 Quantity Rules (done inside run_pipeline_hdfs.py)

* `free_stock = available - reserved`
* `net_demand = max(sold + safety - free_stock, 0)`
* order quantity:

  * at least `MOQ`
  * rounded up to `pack_size` derived from `package`

### 8.3 Optional: Validate with Trino SQL (if Trino catalogs are ready)

```sql
-- supplier in output must match products.supplier_id
SELECT o.sku, o.supplier_id AS supplier_output, p.supplier_id AS supplier_master
FROM hdfs.supplier_orders o
JOIN postgres.public.products p ON o.sku = p.sku
WHERE o.supplier_id <> p.supplier_id;

-- Expected: 0 rows
```

---

## 9. Reprocessing / Run for Past Date

To run for a specific day, set `RUN_DATE`:

```bash
# Example: 2026-01-01

docker exec -e RUN_DATE=2026-01-01 -it orchestrator python /app/scripts/generate_daily_files_hdfs.py

docker exec -e RUN_DATE=2026-01-01 -it orchestrator python /app/scripts/run_pipeline_hdfs.py
```

Note: scripts use **write-once** behavior (they refuse to overwrite existing outputs).

---

## 10. Daily Scheduling (22:00)

Create `run_daily_pipeline.bat` in project root:

```bat
@echo off
cd /d "%~dp0"
docker compose up -d

docker exec -it orchestrator python /app/scripts/generate_daily_files_hdfs.py
docker exec -it orchestrator python /app/scripts/run_pipeline_hdfs.py
```

Schedule this `.bat` in Windows **Task Scheduler** for 22:00 daily.

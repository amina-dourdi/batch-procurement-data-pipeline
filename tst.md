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

✅ Status: **DONE**

Output (example):

* `/raw/orders/2026-01-06/orders.parquet`
* `/raw/stock/2026-01-06/stock.parquet`

Run (Git Bash):

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it orchestrator python /app/scripts/generate_daily_files.py
```

### 6.1 Verify RAW Parquet exists in HDFS

```bash
# List date folders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /raw/orders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /raw/stock

# Check files (replace by the date you see, example 2026-01-06)
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /raw/orders/2026-01-06
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /raw/stock/2026-01-06
```

Expected:

* `orders.parquet`
* `stock.parquet`

---

## 7. Run Pipeline Transformations (HDFS RAW → processed/output → HDFS)

✅ Status: **DONE**

Output (example):

* `/processed/aggregated_orders/2026-01-06/aggregated_orders.parquet`
* `/processed/net_demand/2026-01-06/net_demand.parquet`
* `/output/supplier_orders/2026-01-06/supplier_orders.parquet`

Run (Git Bash):

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it orchestrator python /app/scripts/run_pipeline_hdfs.py
```

### 7.1 Verify outputs exist in HDFS

```bash
# List date folders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /processed/aggregated_orders
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /processed/net_demand
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it namenode hdfs dfs -ls /output/supplier_orders

# Check files (replace by the date you see, example 2026-01-06)
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

To run for a specific day, set `RUN_DATE` (recommended for reprocessing because outputs are **write-once**):

```bash
# Example: 2026-01-01

# Git Bash (disable path conversion)
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -e RUN_DATE=2026-01-01 -it orchestrator python /app/scripts/generate_daily_files.py
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -e RUN_DATE=2026-01-01 -it orchestrator python /app/scripts/run_pipeline_hdfs.py
```

Note:

* If you run again for the **same** date, the pipeline may fail with: `Processed/output déjà existants (write-once).`
* Use a new `RUN_DATE` to reprocess without deleting data.

---

## 10. Daily Scheduling (22:00) using the Orchestrator Container (no Windows task, no .bat)

You want the scheduling to be handled **only by the orchestrator**.

### 10.1 Run the scheduler inside the orchestrator container

1. Make sure containers are up:

```bash
docker compose up -d
```

2. Start the scheduler (it must keep running):

**Git Bash:**

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -it orchestrator python /app/scripts/orchestrator_scheduler.py
```

**PowerShell/CMD:**

```powershell
docker exec -it orchestrator python /app/scripts/orchestrator_scheduler.py
```

### 10.2 Important notes

* The scheduler is an infinite loop (`while True`). If you stop it, the 22:00 job will not run.
* For demo/testing, you can override the schedule time:

```bash
# Example: run at 15:30 today
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker exec -e SCHEDULE_TIME=15:30 -it orchestrator python /app/scripts/orchestrator_scheduler.py
```

### 10.3 Optional: run scheduler as the container command

If you want the scheduler to start automatically with `docker compose up -d`, set the orchestrator service command to:

```yaml
command: python /app/scripts/orchestrator_scheduler.py
```

(Then the orchestrator container will always run the scheduler.)

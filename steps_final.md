Here is the final, complete **`steps.md`** file, updated with the new scheduler times (21:00 & 22:00) and the Orchestrator-based workflow.

---

# Procurement Pipeline: The "Orchestrator" Workflow

This guide explains how to run the Data Engineering pipeline using the `orchestrator` container as your command center. It covers Master Data setup, Chaos Data Generation (Avro), and the Trino Processing Pipeline.

---

## 1. Project Structure

Ensure your project folder on your host machine has this exact structure:

```text
/procurement_project/
│
├── scripts/
│   ├── generate_master_data.py    # (Generates Master CSVs)
│   ├── generate_daily_files.py    # (Chaos Generator: Avro + Fake Errors)
│   ├── run_pipeline_hdfs.py       # (Super Script: Validation + Trino SQL)
│   ├── orchestrator_scheduler.py  # (Automation: Runs at 21:00 & 22:00)
│   ├── hdfs_client.py             # (Helper: WebHDFS)
│   └── pg_client.py               # (Helper: Postgres)
│
├── setup_db.sh                    # (Database Setup - Runs on Host)
└── docker-compose.yml             # (Infrastructure)

```

---

## 2. Interactive Mode (Prerequisite)

We will execute all Python scripts **inside** the orchestrator container. This prevents environment issues on your local machine.

**1. Enter the Orchestrator:**
Open your terminal and run:

```bash
docker exec -it orchestrator bash

```

*(You are now inside the container. Your prompt should look like `root@orchestrator:/app#`)*

**2. Install Required Libraries:**
Run this once inside the container to install the tools needed for Avro, Trino, and HDFS:

```bash
pip install "numpy<2"

pip install  pandavro fastavro trino

```

---

## 3. Phase 1: Master Data Setup

This phase creates the "Golden Records" (Suppliers, Products, Markets) and loads them into PostgreSQL.

### Step 1: Generate CSVs (Inside Orchestrator)

Run this inside the container to generate the CSV files.

```bash
python scripts/generate_master_data.py

```

*Expected Output:* `✔ Created ... products.`

### Step 2: Load to PostgreSQL (From Host Machine)

**⚠️ IMPORTANT:** Do **not** run this inside the container. Open a **new** terminal window on your normal computer (Host) and run:

```bash
./setup_db.sh

```

*(This script uses `docker cp` to move data into Postgres, so it must run from outside).*

---

## 4. Phase 2: The Daily Workflow

Back inside your **Orchestrator** terminal, run the daily pipeline sequence manually (for testing).

### Step A: Generate Chaos Data (Ingestion)

This script creates the daily folder in HDFS (`/raw/orders/...`).

* It generates **Avro** files.
* It intentionally creates **corrupted files** (JSON/Text masked as Avro) to simulate real-world data issues.

```bash

$ docker exec -it namenode bash
root@namenode:/# hdfs dfsadmin -safemode leave
Safe mode is OFF
root@namenode:/# hdfs dfs -chmod -R 777 /raw


python scripts/generate_daily_files.py

```

*Expected Output:*

```text
📦 Processing 10 markets...
✅ Uploaded orders_MKT-001.avro [OK]
☠️ [Simulated Error] MKT-004 sent CORRUPTED content...

```

### Step B: Run Pipeline (Validate + Process)

This "Super Script" performs two critical actions:

1. **Validation (The Bouncer):** Scans HDFS, detects the corrupted text/json files, and moves them to `/errors`.
2. **Processing (The Brain):** Connects to **Trino**, joins the valid Avro data with Postgres, and saves the final orders as Parquet.

```bash
python scripts/run_pipeline_hdfs.py

```

*Expected Output:*

```text
--- 🐘 STARTING PIPELINE FOR 2026-01-13 ---
🔍 Validating files in /raw/orders/2026-01-13...
   ✅ orders_MKT-001.avro is valid.
   ❌ CORRUPT FILE DETECTED: orders_MKT-004.avro
      -> Moved to /errors/orders/2026-01-13
🧠 Calculating Net Demand (Distributed Join)...
✅ Success! Orders generated in HDFS: /output/supplier_orders/2026-01-13

```

---

## 5. Phase 3: Automation (Scheduler)

To make this run automatically at **21:00** and **22:00**, run the scheduler inside the orchestrator.

```bash
python scripts/orchestrator_scheduler.py

```

* **Status:** It will print `⏰ Orchestrator started. Scheduled times: 21:00, 22:00`.
* **Action:** Leave this terminal window open. The script will sleep and trigger the workflow automatically when the time matches.

---

## 6. How to Verify Results

You can verify the processed data using the `trino` command line tool inside the orchestrator.

**Check the Final Orders:**

```bash
trino --server trino:8080 --catalog hive --schema default --execute "
    SELECT * FROM supplier_orders_2026_01_13 LIMIT 5
"

```

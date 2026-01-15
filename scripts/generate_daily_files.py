import os
import random
import pandas as pd
from datetime import date
from hdfs_client import WebHDFSClient
from pg_client import read_sql_df
import json
import pandavro as pdx  # pip install pandavro
from trino.dbapi import connect
from trino_utils import ensure_schema

DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

MAX_SKUS_PER_MARKET = int(os.getenv("MAX_SKUS_PER_MARKET", "40"))
LOCATIONS = os.getenv("LOCATIONS", "WH1,WH2,WH3").split(",")

# PROBABILITIES (The Chaos Factors)
PROB_MISSING_FILE = 0.10  # 10% chance a market forgets to send file
PROB_GHOST_SKU = 0.05     # 5% chance they sell an unknown product


def main():
    hdfs = WebHDFSClient(HDFS_BASE_URL, user=HDFS_USER)

    # Create schemas for orders and stock
    ensure_schema("raw_orders")
    ensure_schema("raw_stock")

    # --- master data depuis Postgres ---
    df_markets = read_sql_df("SELECT market_id FROM market;")
    df_products = read_sql_df("SELECT sku, supplier_id, moq, package FROM products;")
    
    market_ids = df_markets["market_id"].dropna().unique().tolist()
    valid_skus = df_products["sku"].dropna().unique().tolist()

    # =========================================================
    # =============== RAW ORDERS (PER MARKET) =================
    # =========================================================

    # We use the date as seed so every run for the SAME DATE produces SAME errors
    random.seed(f"chaos-{RUN_DATE}")

    hdfs_orders_dir = f"/raw/orders/{RUN_DATE}"
    hdfs.mkdirs(hdfs_orders_dir)

    local_dir_orders = os.path.join(DATA_ROOT, "raw/orders", RUN_DATE)
    local_dir_stock = os.path.join(DATA_ROOT, "raw/stock", RUN_DATE)
    os.makedirs(local_dir_orders, exist_ok=True)
    os.makedirs(local_dir_stock, exist_ok=True)

    print(f" Processing {len(market_ids)} markets...")

    for market_id in market_ids:

        # --- CHAOS 1: MISSING FILE ---
        # Roll the dice: Does this market send the file today?
        if random.random() < PROB_MISSING_FILE:
            print(f" [Simulated Error] Market {market_id} did NOT send a file.")
            continue  # Skip to next market

        orders_rows = []

        sold_skus = random.sample(valid_skus, k=min(MAX_SKUS_PER_MARKET, len(valid_skus)))


        for sku in sold_skus:
            qty = random.randint(1, 12)
            orders_rows.append({
                "market_id": market_id,
                "sku": sku,
                "quantity": qty,
                "timestamp": f"{RUN_DATE}T10:00:00"
            })

        # --- CHAOS 2: GHOST SKU (Unknown Product) ---
        if random.random() < PROB_GHOST_SKU:
            print(f"[Simulated Error] Market {market_id} sold a Ghost SKU.")
            orders_rows.append({
                "market_id": market_id,
                "sku": "SKU-99999-GHOST",  # Not in Postgres
                "quantity": 50,
                "timestamp": f"{RUN_DATE}T12:00:00"
            })

        # Define Paths
        filename = f"orders_{market_id}.avro"
        local_path = os.path.join(local_dir_orders, filename)
        hdfs_path = f"{hdfs_orders_dir}/{filename}"

        # ---  GENERATE VALID AVRO ---
        if orders_rows:
            df_market = pd.DataFrame(orders_rows)
            pdx.to_avro(local_path, df_market)
                
            # --- UPLOAD TO HDFS ---
            if hdfs.exists(hdfs_path):
                print(f" Skipping existing: {filename}")
            else:
                hdfs.put_file(local_path, hdfs_path, overwrite=False)
                print(f" Uploaded {filename} [OK]")
        else:
            print(f" Market {market_id} had 0 orders.")
            


    # =========================================================
    # ===================== RAW STOCK =========================
    # =========================================================
    random.seed("stock-" + RUN_DATE)

    stock_rows = []
    for sku in valid_skus:
        available = random.randint(0, 200)
        reserved = random.randint(0, min(50, available))
        safety = random.randint(5, 40)
        stock_rows.append({
            "run_date": RUN_DATE,
            "sku": sku,
            "quantity_available": available,
            "quantity_reserved": reserved,
            "safety_quantity": safety,
            "location": random.choice(LOCATIONS)
        })

    df_stock = pd.DataFrame(stock_rows)

    # ---- LOCAL ----
    local_stock = os.path.join(local_dir_stock, "stock.avro")
    pdx.to_avro(local_stock, df_stock)

    # ---- HDFS ----
    hdfs_stock_dir = f"/raw/stock/{RUN_DATE}"
    hdfs.mkdirs(hdfs_stock_dir)

    hdfs_stock_path = f"{hdfs_stock_dir}/stock.avro"

    if hdfs.exists(hdfs_stock_path):
        print(f" Stock file already exists: {hdfs_stock_path}")
    else:
        hdfs.put_file(local_stock, hdfs_stock_path, overwrite=False)
        print(f"[OK] HDFS stock -> {hdfs_stock_path}")

if __name__ == "__main__":
    main()

import os
import random
import pandas as pd
from datetime import date
from hdfs_client import WebHDFSClient
from pg_client import read_sql_df

DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

MAX_SKUS_PER_MARKET = int(os.getenv("MAX_SKUS_PER_MARKET", "40"))
LOCATIONS = os.getenv("LOCATIONS", "WH1,WH2,WH3").split(",")

def main():
    hdfs = WebHDFSClient(HDFS_BASE_URL, user=HDFS_USER)

    # --- master data depuis Postgres ---
    df_markets = read_sql_df("SELECT market_id FROM market;")
    df_products = read_sql_df("SELECT sku, supplier_id, moq, package FROM products;")
    #df_suppliers = read_sql_df("SELECT supplier_id FROM suppliers;")

    #bad = df_products[~df_products["supplier_id"].isin(df_suppliers["supplier_id"])]
    # if not bad.empty:
    #     raise ValueError("products.supplier_id contient des ids inexistants dans suppliers.")

    market_ids = df_markets["market_id"].dropna().unique().tolist()
    skus = df_products["sku"].dropna().unique().tolist()

    # =========================================================
    # =============== RAW ORDERS (PER MARKET) =================
    # =========================================================
    random.seed("orders-" + RUN_DATE)

    hdfs_orders_dir = f"/raw/orders/{RUN_DATE}"
    hdfs.mkdirs(hdfs_orders_dir)

    local_dir = os.path.join(DATA_ROOT, "raw", RUN_DATE)
    os.makedirs(local_dir, exist_ok=True)

    for market_id in market_ids:

        orders_rows = []

        sold_skus = random.sample(skus, k=min(MAX_SKUS_PER_MARKET, len(skus)))

        for sku in sold_skus:
            qty = random.randint(0, 12)
            if qty > 0:
                orders_rows.append({
                    "run_date": RUN_DATE,
                    "market_id": market_id,
                    "sku": sku,
                    "quantity_sold": qty
                })

        df_orders_market = pd.DataFrame(orders_rows)

        # ---- LOCAL ----
        local_orders = os.path.join(
            local_dir, f"orders_market_{market_id}.parquet"
        )
        df_orders_market.to_parquet(local_orders, index=False)

        # ---- HDFS ----
        hdfs_orders_path = f"{hdfs_orders_dir}/orders_market_{market_id}.parquet"

        if hdfs.exists(hdfs_orders_path):
            raise RuntimeError(f"File already exists: {hdfs_orders_path}")

        hdfs.put_file(local_orders, hdfs_orders_path, overwrite=False)

        print(f"[OK] HDFS orders -> {hdfs_orders_path}")

    # =========================================================
    # ===================== RAW STOCK =========================
    # =========================================================
    random.seed("stock-" + RUN_DATE)

    stock_rows = []
    for sku in skus:
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
    local_stock = os.path.join(local_dir, "stock.parquet")
    df_stock.to_parquet(local_stock, index=False)

    # ---- HDFS ----
    hdfs_stock_dir = f"/raw/stock/{RUN_DATE}"
    hdfs.mkdirs(hdfs_stock_dir)

    hdfs_stock_path = f"{hdfs_stock_dir}/stock.parquet"

    if hdfs.exists(hdfs_stock_path):
        raise RuntimeError("Stock file already exists for this date.")

    hdfs.put_file(local_stock, hdfs_stock_path, overwrite=False)

    print(f"[OK] HDFS stock -> {hdfs_stock_path}")

if __name__ == "__main__":
    main()

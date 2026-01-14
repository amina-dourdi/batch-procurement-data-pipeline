import os
import pandas as pd
import pandavro as pdx
from datetime import date
from trino.dbapi import connect
from hdfs_client import WebHDFSClient
from pg_client import read_sql_df 
from collections import defaultdict
import json

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")

TRINO_HOST = os.getenv("TRINO_HOST",'trino')
TRINO_PORT = int(os.getenv("TRINO_PORT", 8080))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "hive")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "default")

HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

def main(guard=None):
    hdfs = WebHDFSClient(HDFS_BASE_URL, user=HDFS_USER)

    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )    
    cur = conn.cursor()

    print("Checking schemas...")
    cur.execute("CREATE SCHEMA IF NOT EXISTS hive.default")
    
    print(" Fetching Products from Postgres (Bypassing Trino Catalog)...")
    
    df_products = read_sql_df("SELECT sku, supplier_id, moq, package FROM products")
    
    df_products['moq'] = df_products['moq'].fillna(1).astype(int)
    df_products['package'] = df_products['package'].fillna("Single Unit").astype(str)
    
    local_prod_path = os.path.join(DATA_ROOT, "temp_products.avro")
    pdx.to_avro(local_prod_path, df_products)
    
    hdfs_prod_dir = f"/raw/products/{RUN_DATE}"
    hdfs_prod_path = f"{hdfs_prod_dir}/products.avro"
    
    hdfs.mkdirs(hdfs_prod_dir)
    hdfs.put_file(local_prod_path, hdfs_prod_path, overwrite=True)
    print(" Products uploaded to HDFS.")

    #  Creating a Temporary Products Table

    cur.execute("DROP TABLE IF EXISTS hive.default.temp_products_today")
    
    cur.execute(f"""
        CREATE TABLE hive.default.temp_products_today (
            sku VARCHAR,
            supplier_id VARCHAR,
            moq BIGINT,
            package VARCHAR
        ) WITH (
            format = 'AVRO',
            external_location = 'hdfs://namenode:9000/raw/products/{RUN_DATE}'
        )
    """)
    
    table_src_net = f"hive.processed.net_demand_{RUN_DATE.replace('-', '_')}"
    hdfs_target_dir = f"/output/supplier_orders/{RUN_DATE}"
    
    table_dest = f"hive.default.supplier_orders_{RUN_DATE.replace('-', '_')}"
    # ------------------------------------------------------------

    print(f"Cleaning up target directory: {hdfs_target_dir}")
    hdfs.delete(hdfs_target_dir, recursive=True)

    print(f"Generating Supplier Orders into {table_dest}...")
    cur.execute(f"DROP TABLE IF EXISTS {table_dest}")

    query_final = f"""
    CREATE TABLE {table_dest}
    WITH (
        format = 'PARQUET', 
        external_location = '{hdfs_target_dir}'
    )
    AS 
    SELECT 
        nd.run_date,
        p.supplier_id,
        nd.sku,
        CAST(
            CEILING(
                CAST(GREATEST(nd.net_demand, COALESCE(p.moq, 1)) AS DOUBLE) / 
                (CASE 
                    WHEN p.package LIKE '%Box of 6%' THEN 6 
                    WHEN p.package LIKE '%Box of 12%' THEN 12 
                    WHEN p.package LIKE '%Box of 24%' THEN 24 
                    WHEN p.package LIKE '%Pallet%' THEN 100 
                    ELSE 1 
                END)
            ) 
            * (CASE 
                WHEN p.package LIKE '%Box of 6%' THEN 6 
                WHEN p.package LIKE '%Box of 12%' THEN 12 
                WHEN p.package LIKE '%Box of 24%' THEN 24 
                WHEN p.package LIKE '%Pallet%' THEN 100 
                ELSE 1 
            END) 
        AS INTEGER) as quantity
    FROM {table_src_net} nd
    JOIN hive.default.temp_products_today p ON nd.sku = p.sku
    WHERE nd.net_demand > 0
    """
    
    try:
        cur.execute(query_final)

        # Added

        OUTPUT_LOCAL_DIR = f"{DATA_ROOT}/output/supplier_orders/{RUN_DATE}"  # Local copy
        OUTPUT_HDFS_DIR = f"/output/supplier_orders/{RUN_DATE}"       # HDFS copy

        os.makedirs(OUTPUT_LOCAL_DIR, exist_ok=True)
        hdfs.mkdirs(OUTPUT_HDFS_DIR)  # Make sure the HDFS folder exists
        cur.execute(f"""
        SELECT run_date, supplier_id, sku, quantity
        FROM {table_dest}
        """)

        supplier_orders = defaultdict(list)
        rows_table = cur.fetchall()  # After running your final query

        for run_date, supplier_id, sku, qty in rows_table:
            supplier_orders[supplier_id].append({
                "sku": sku,
                "quantity": int(qty)
            })

        # Write each supplier file locally AND to HDFS
        for supplier_id, items in supplier_orders.items():
            order = {
                "supplier_id": supplier_id,
                "run_date": RUN_DATE,
                "items": items
            }

            # Local file
            local_file_path = f"{OUTPUT_LOCAL_DIR}/{supplier_id}.json"
            with open(local_file_path, "w") as f:
                json.dump(order, f, indent=2)

            # HDFS file
            hdfs_file_path = f"{OUTPUT_HDFS_DIR}/{supplier_id}.json"
            hdfs.put_file(local_file_path, hdfs_file_path, overwrite=True)
##########
        
        print(rows_table)
        # for run_date, supplier_id, sku, qty in rows_table:
        #     supplier_orders[supplier_id].append({
        #         "sku": sku,
        #         "quantity": int(qty)
        #     })
        print(supplier_orders)
        # OUTPUT_DIR = f"/tmp/supplier_orders/{RUN_DATE}"
        # os.makedirs(OUTPUT_DIR, exist_ok=True)

        # for supplier_id, items in supplier_orders.items():
        #     order = {
        #         "supplier_id": supplier_id,
        #         "run_date": RUN_DATE,
        #         "items": items
        #     }

        #     file_path = f"{OUTPUT_DIR}/{supplier_id}.json"
        #     with open(file_path, "w") as f:
        #         json.dump(order, f, indent=2)

        #     print(f"✅ File created: {file_path}")
        # ###
        print(f" Success! Orders generated in HDFS: {hdfs_target_dir}")
    except Exception as e:
        print(f" Error in Supplier Orders generation: {e}")
        raise e

    # --- CHECK PACKAGE COMPLIANCE ---
    if guard:
        print("🔍 Verifying Package Size Compliance...")
        cur.execute(f"SELECT supplier_id, sku, quantity FROM {table_dest}")
        rows = cur.fetchall()
        
        if not rows:
            print("  No orders generated (Result is empty).")
        
        for supplier_id, sku, qty in rows:
            order_ref = f"PO-{supplier_id}-{RUN_DATE}"
            guard.check_package_compliance(order_id=order_ref, sku=sku, quantity=qty)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
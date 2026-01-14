import os
from datetime import date
import pandas as pd
from trino.dbapi import connect
import pandavro as pdx  # pip install pandavro

# from hdfs import InsecureClient
# import pyarrow as pa
# import pyarrow.parquet as pq
from hdfs_client import WebHDFSClient
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

# HDFS client
HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
HDFS_USER = 'hive'                  # user with write access
# client = InsecureClient(HDFS_BASE, user=HDFS_USER)
hdfs = WebHDFSClient(HDFS_BASE_URL, user=HDFS_USER)

# Trino connection
conn = connect(
    host="localhost",
    port=8080,
    user="admin",
    catalog='hive',      # using Trino catalog to query Postgres/HDFS
    schema='default'
)

# --- Step 1: read net demand and product info ---
query = f"""
SELECT 
    nd.run_date,
    p.supplier_id,
    nd.sku,
    CAST(
        CEIL(
            GREATEST(nd.net_demand, p.moq) /
            CASE p.package
                WHEN 'Single Unit' THEN 1
                WHEN 'Box of 6' THEN 6
                WHEN 'Box of 12' THEN 12
                WHEN 'Box of 24' THEN 24
                WHEN 'Pallet' THEN 480
            END
        )
        *
        CASE p.package
            WHEN 'Single Unit' THEN 1
            WHEN 'Box of 6' THEN 6
            WHEN 'Box of 12' THEN 12
            WHEN 'Box of 24' THEN 24
            WHEN 'Pallet' THEN 480
        END
    AS INTEGER) AS order_qty
FROM hive.processed.net_demand_{RUN_DATE.replace('-', '_')} nd
JOIN postgresql.public.products p ON nd.sku = p.sku
WHERE nd.net_demand > 0
"""

df = pd.read_sql(query, conn)

# --- Step 2: write one file per supplier ---
base_hdfs_path = f'/output/supplier_orders/{RUN_DATE}/'
# client.makedirs(base_hdfs_path, exist_ok=True)

for supplier_id, df_supplier in df.groupby('supplier_id'):
    hdfs_path = f'{base_hdfs_path}/{supplier_id}.parquet'
    #table = pa.Table.from_pandas(df_supplier)
    # We create the DataFrame
    df = pd.DataFrame(df_supplier)
    
    # Save as Avro using pandavro
    #pdx.to_avro(local_path, df_market)
            
    # --- UPLOAD TO HDFS ---
    if hdfs.exists(hdfs_path):
        print(f" Skipping existing: {hdfs_path}")
    else:
        hdfs.put_file(local_path, hdfs_path, overwrite=False)
    print(f"✅ Supplier order for {supplier_id} written to {hdfs_path}")

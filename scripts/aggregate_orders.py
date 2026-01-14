import os
from datetime import date
from trino.dbapi import connect
from hdfs_client import WebHDFSClient 
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
TRINO_HOST = os.environ["TRINO_HOST"]
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

    # ---  FIX: CREATE SCHEMAS FIRST (Lignes de ton ami) ---
    print("Checking schemas...")
    cur.execute("CREATE SCHEMA IF NOT EXISTS hive.default")
    cur.execute("CREATE SCHEMA IF NOT EXISTS hive.processed")


    # On définit le chemin EXACT où ton autre fichier a écrit les données
    # C'est ici que tu fais le lien avec generate_daily_files.py
    hdfs_raw_path = f"/raw/orders/{RUN_DATE}/"
    hdfs_target_dir = f"/processed/aggregated_orders/{RUN_DATE}"
    table_agg = f"hive.processed.aggregated_orders_{RUN_DATE.replace('-', '_')}"

    # 1. On crée une table de passage pour lire l'AVRO que tu viens de générer
    # On l'appelle 'temp_raw_orders'
    cur.execute("DROP TABLE IF EXISTS hive.default.temp_raw_orders")
    
    setup_raw_query = f"""
    CREATE TABLE  hive.default.temp_raw_orders (
        market_id VARCHAR,
        sku VARCHAR,
        quantity BIGINT,
        timestamp VARCHAR
    )
    WITH (
        format = 'AVRO',
        external_location = '{hdfs_raw_path}'
    )
    """
    cur.execute(setup_raw_query)

    print(f"Cleaning up target directory: {hdfs_target_dir}")
    hdfs.delete(hdfs_target_dir, recursive=True)

    # 2. Maintenant on fait l'agrégation vers le PARQUET
    cur.execute(f"DROP TABLE IF EXISTS {table_agg}")
    
    query_agg = f"""
    CREATE TABLE {table_agg}
    WITH (
        format = 'PARQUET', 
        external_location = '/processed/aggregated_orders/{RUN_DATE}/'
    )
    AS 
    SELECT sku, sum(quantity) as total_quantity 
    FROM hive.default.temp_raw_orders 
    GROUP BY sku
    """
    print(f"Étape 1 : Agrégation des fichiers Avro de {hdfs_raw_path} vers {table_agg}")
    cur.execute(query_agg)

    # 3. VÉRIFICATION DATA QUALITY
    if guard:
        cur.execute(f"SELECT sku, total_quantity FROM {table_agg}")
        aggregated_results = cur.fetchall()
        for sku, qty in aggregated_results:
            guard.check_order_magnitude(order_id=f"AGG-{RUN_DATE}", sku=sku, quantity=qty)

    cur.close()
    conn.close()
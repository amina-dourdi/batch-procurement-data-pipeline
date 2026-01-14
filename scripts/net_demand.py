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
    # 1. Connexion à Trino
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )    
    cur = conn.cursor()

    # --- 🛠️ FIX: CREATE SCHEMAS FIRST (Lignes de ton ami) ---
    print("Checking schemas...")
    cur.execute("CREATE SCHEMA IF NOT EXISTS hive.default")
    cur.execute("CREATE SCHEMA IF NOT EXISTS hive.processed")

    # La suite de ton code reste la même...
    hdfs_stock_path = f"/raw/stock/{RUN_DATE}/"
    table_src_agg = f"hive.processed.aggregated_orders_{RUN_DATE.replace('-', '_')}"
    hdfs_target_dir = f"/processed/net_demand/{RUN_DATE}"
    table_dest = f"hive.processed.net_demand_{RUN_DATE.replace('-', '_')}"

    # --- ÉTAPE A : Créer le pont vers le fichier STOCK Avro généré ---
    cur.execute("DROP TABLE IF EXISTS hive.default.temp_raw_stock")
    setup_stock_query = f"""
    CREATE TABLE hive.default.temp_raw_stock (
        run_date VARCHAR,
        sku VARCHAR,
        quantity_available BIGINT,
        quantity_reserved BIGINT,
        safety_quantity BIGINT,
        location VARCHAR
    )
    WITH (
        format = 'AVRO',
        external_location = '{hdfs_stock_path}'
    )
    """
    cur.execute(setup_stock_query)
    print(f"Cleaning up target directory: {hdfs_target_dir}")
    hdfs.delete(hdfs_target_dir, recursive=True)
    
    # --- ÉTAPE B : Calcul de la demande nette ---
    cur.execute(f"DROP TABLE IF EXISTS {table_dest}")
    
    # On utilise 'temp_raw_stock' au lieu de 'hive.raw.stock'
    query_net = f"""
    CREATE TABLE {table_dest}
    WITH (
        format = 'PARQUET', 
        external_location = '/processed/net_demand/{RUN_DATE}/'
    )
    AS 
    SELECT 
        '{RUN_DATE}' as run_date,
        ao.sku,
        (ao.total_quantity + s.safety_quantity - (s.quantity_available - s.quantity_reserved)) as net_demand
    FROM {table_src_agg} ao
    JOIN hive.default.temp_raw_stock s ON ao.sku = s.sku
    """
    print(f"Étape 2 : Calcul de la demande nette à partir du stock {hdfs_stock_path}")
    cur.execute(query_net)

    # --- ÉTAPE C : VÉRIFICATION DATA QUALITY ---
    if guard:
        print("Vérification de la cohérence des stocks...")
        cur.execute("SELECT sku, quantity_available, quantity_reserved FROM hive.default.temp_raw_stock WHERE quantity_reserved > quantity_available")
        anomalies = cur.fetchall()
        for sku, avail, res in anomalies:
            guard.check_stock_logic(sku=sku, available=avail, reserved=res)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
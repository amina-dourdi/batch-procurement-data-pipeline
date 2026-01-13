import os
from datetime import date
from trino.dbapi import connect

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

def main(guard=None):
    # 1. Connexion à Trino
    conn = connect(host="localhost", port=8080, user="admin", catalog='hive', schema='default')
    cur = conn.cursor()

    # Définition des chemins et tables
    hdfs_stock_path = f"/raw/stock/{RUN_DATE}/"
    table_src_agg = f"hive.processed.aggregated_orders_{RUN_DATE.replace('-', '_')}"
    table_dest = f"hive.processed.net_demand_{RUN_DATE.replace('-', '_')}"

    # --- ÉTAPE A : Créer le pont vers le fichier STOCK Avro généré ---
    cur.execute("DROP TABLE IF EXISTS hive.default.temp_raw_stock")
    setup_stock_query = f"""
    CREATE TABLE hive.default.temp_raw_stock (
        run_date VARCHAR,
        sku VARCHAR,
        quantity_available INTEGER,
        quantity_reserved INTEGER,
        safety_quantity INTEGER,
        location VARCHAR
    )
    WITH (
        format = 'AVRO',
        external_location = '{hdfs_stock_path}'
    )
    """
    cur.execute(setup_stock_query)

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
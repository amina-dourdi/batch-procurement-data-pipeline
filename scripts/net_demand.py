import os
from datetime import date
from trino.dbapi import connect

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

def main(guard=None):
    # 1. Connexion à Trino
    conn = connect(host="localhost", port=8080, user="admin", catalog='hive', schema='default')
    cur = conn.cursor()

    # Tables : Source (Aggregated) et Destination (Net Demand)
    table_src = f"hive.processed.aggregated_orders_{RUN_DATE.replace('-', '_')}"
    table_dest = f"hive.processed.net_demand_{RUN_DATE.replace('-', '_')}"

    # 2. Nettoyage et calcul de la demande nette
    cur.execute(f"DROP TABLE IF EXISTS {table_dest}")

    query = f"""
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
    FROM {table_src} ao
    JOIN hive.raw.stock s ON ao.sku = s.sku
    """
    print(f"📉 Étape 2 : Calcul de la demande nette dans {table_dest}")
    cur.execute(query)

    # 3. VÉRIFICATION DATA QUALITY (Logique de Stock)
    if guard:
        print("🔍 Vérification de la cohérence des stocks (Réservé vs Disponible)...")
        # On interroge la table brute de stock pour vérifier les erreurs logiques
        # On ne vérifie que les lignes qui ont un problème pour être rapide
        query_check = """
            SELECT sku, quantity_available, quantity_reserved 
            FROM hive.raw.stock 
            WHERE quantity_reserved > quantity_available
        """
        cur.execute(query_check)
        anomalies = cur.fetchall()

        for sku, avail, res in anomalies:
            # On appelle ta fonction de data_quality.py
            guard.check_stock_logic(sku=sku, available=avail, reserved=res)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
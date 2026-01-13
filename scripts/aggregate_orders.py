import os
from datetime import date
from trino.dbapi import connect

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

def main(guard=None):
    # 1. Connexion à Trino
    conn = connect(host="localhost", port=8080, user="admin", catalog='hive', schema='default')
    cur = conn.cursor()

    table_agg = f"hive.processed.aggregated_orders_{RUN_DATE.replace('-', '_')}"
    
    # 2. Nettoyage et création du fichier Parquet agrégé via CTAS
    cur.execute(f"DROP TABLE IF EXISTS {table_agg}")
    
    query = f"""
    CREATE TABLE {table_agg}
    WITH (
        format = 'PARQUET', 
        external_location = '/processed/aggregated_orders/{RUN_DATE}/'
    )
    AS 
    SELECT sku, sum(quantity) as total_quantity 
    FROM hive.raw.orders_avro 
    WHERE run_date = '{RUN_DATE}' 
    GROUP BY sku
    """
    print(f"Étape 1 : Agrégation des ventes dans {table_agg}")
    cur.execute(query)

    # 3. VÉRIFICATION DATA QUALITY (Magnitude)
    # Si l'objet guard est passé, on vérifie les résultats
    if guard:
        print("Vérification de la magnitude des commandes (MxOQ)...")
        # On lit les résultats que Trino vient de calculer
        cur.execute(f"SELECT sku, total_quantity FROM {table_agg}")
        aggregated_results = cur.fetchall()

        for sku, qty in aggregated_results:
            # On appelle ta fonction de data_quality.py
            # entity_id est ici le SKU agrégé pour la journée
            guard.check_order_magnitude(order_id=f"AGG-{RUN_DATE}", sku=sku, quantity=qty)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
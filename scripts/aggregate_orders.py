import os
from datetime import date
from trino.dbapi import connect

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

def main():
    conn = connect(host="localhost", port=8080, user="admin", catalog='hive', schema='default')
    cur = conn.cursor()

    table_agg = f"hive.processed.aggregated_orders_{RUN_DATE.replace('-', '_')}"
    
    # Nettoyage et création du fichier Parquet agrégé
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
    print(f"Étape 1 : Agrégation des ventes dans /processed/aggregated_orders/{RUN_DATE}")
    cur.execute(query)

if __name__ == "__main__":
    main()
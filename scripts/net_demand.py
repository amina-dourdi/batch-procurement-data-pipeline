import os
from datetime import date
from trino.dbapi import connect

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

def main():
    conn = connect(host="localhost", port=8080, user="admin", catalog='hive', schema='default')
    cur = conn.cursor()

    # Tables : Source (Aggregated) et Destination (Net Demand)
    table_src = f"hive.processed.aggregated_orders_{RUN_DATE.replace('-', '_')}"
    table_dest = f"hive.processed.net_demand_{RUN_DATE.replace('-', '_')}"

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
    print(f"Étape 2 : Calcul de la demande nette dans /processed/net_demand/{RUN_DATE}")
    cur.execute(query)

if __name__ == "__main__":
    main()
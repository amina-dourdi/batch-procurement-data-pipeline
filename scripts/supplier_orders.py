import os
from datetime import date
from trino.dbapi import connect

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

def main():
    conn = connect(host="localhost", port=8080, user="admin", catalog='hive', schema='default')
    cur = conn.cursor()

    # Source : Net Demand (Parquet colonnaire sur HDFS)
    table_src = f"hive.processed.net_demand_{RUN_DATE.replace('-', '_')}"

    query = f"""
    INSERT INTO hive.output.supplier_orders
    SELECT 
        nd.run_date, 
        p.supplier_id, 
        nd.sku, 
        CAST(CEIL(GREATEST(nd.net_demand, p.moq)) AS INTEGER)
    FROM {table_src} nd
    JOIN postgresql.public.products p ON nd.sku = p.sku
    WHERE nd.net_demand > 0
    """
    print(f"🚀 Étape 3 : Commandes fournisseurs générées dans /output/supplier_orders/{RUN_DATE}")
    cur.execute(query)

if __name__ == "__main__":
    main()
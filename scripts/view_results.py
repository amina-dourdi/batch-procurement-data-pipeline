
import os
from datetime import date
from trino.dbapi import connect

TRINO_HOST = os.getenv("TRINO_HOST",'trino')
TRINO_PORT = int(os.getenv("TRINO_PORT", 8080))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "hive")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "default")



RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
TABLE_NAME = f"hive.processed.aggregated_orders_{RUN_DATE.replace('-', '_')}"

print(f"---  INSPECTING FINAL RESULTS: {TABLE_NAME} ---")

try:
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )    
    cur = conn.cursor()
    
    print(" Querying data...")
    cur.execute(f"SELECT * FROM {TABLE_NAME}  ")
    rows = cur.fetchall()
    
    if rows:
        print(f"\n✅ SUCCESS! Found data (Showing first 20 rows):")
        print("=" * 70)
        # Print name of column
        print(f"{'Run Date':<12} | {'Supplier ID':<15} | {'SKU':<15} | {'Quantity':<10}")
        print("-" * 70)
        
        # print rows
        for row in rows:
            r_date = str(row[0])
            r_supp = str(row[1])
            r_sku = str(row[2])
            r_qty = str(row[3])
            
            print(f"{r_date:<12} | {r_supp:<15} | {r_sku:<15} | {r_qty:<10}")
        print("=" * 70)
    else:
        print(" The table exists but contains NO DATA.")
        print("   This might mean Net Demand was 0 for all products.")

except Exception as e:
    print(f"\n Error reading data: {e}")
    if "does not exist" in str(e):
        print("   💡 Hint: The pipeline might not have finished successfully yet.")


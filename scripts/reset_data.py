
import os
from datetime import date
from hdfs_client import WebHDFSClient

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")

print(f"---  CLEANING HDFS DATA FOR {RUN_DATE} ---")

try:
    hdfs = WebHDFSClient(HDFS_BASE_URL, user="root")
    
    paths_to_delete = [
        f"/raw/orders/{RUN_DATE}",
        f"/raw/stock/{RUN_DATE}",
        f"/processed/aggregated_orders/{RUN_DATE}",
        f"/processed/net_demand/{RUN_DATE}",
        f"/output/supplier_orders/{RUN_DATE}",
        f"/errors/orders/{RUN_DATE}"
    ]

    for path in paths_to_delete:
        print(f"   🗑️ Deleting: {path}")
        hdfs.delete(path, recursive=True)

    print("\n HDFS is clean. You can now run the pipeline.")

except Exception as e:
    print(f" Error: {e}")

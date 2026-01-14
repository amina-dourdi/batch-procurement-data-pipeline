import os
from datetime import date
from hdfs_client import WebHDFSClient
import requests

RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")

DOWNLOAD_TARGETS = [
    {
        "name": "aggregated_orders",
        "hdfs_source": f"/processed/aggregated_orders/{RUN_DATE}",
        "local_parent_folder": "processed/aggregated_orders",
        "extension": ".parquet"
    },
    {
        "name": "net_demand",
        "hdfs_source": f"/processed/net_demand/{RUN_DATE}",
        "local_parent_folder": "processed/net_demand",
        "extension": ".parquet"
    },
    {
        "name": "supplier_orders",
        "hdfs_source": f"/output/supplier_orders/{RUN_DATE}",
        "local_parent_folder": "output/supplier_orders",
        "extension": ".parquet"
    },
    {
        "name": "exceptions",
        "hdfs_source": f"/logs/exceptions/date={RUN_DATE}",
        "local_parent_folder": "logs/exceptions",
        "extension": ".csv"
    }
]

def list_files(hdfs, path):
    try:
        url = f"{HDFS_BASE_URL}/webhdfs/v1{path}?op=LISTSTATUS"
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if 'FileStatuses' in data and 'FileStatus' in data['FileStatuses']:
                return [
                    f['pathSuffix'] for f in data['FileStatuses']['FileStatus'] 
                    if f['type'] == 'FILE' and not f['pathSuffix'].startswith('.')
                ]
        return []
    except Exception:
        return []

def main():
    print(f"---  DOWNLOADING & RENAMING FILES ({RUN_DATE}) ---")
    hdfs = WebHDFSClient(HDFS_BASE_URL, user="root")
    
    for target in DOWNLOAD_TARGETS:
        name = target['name']
        hdfs_src = target['hdfs_source']
        local_folder = os.path.join(DATA_ROOT, target['local_parent_folder'])
        expected_ext = target['extension']
        
        print(f"\n Processing: {name}")
        
        os.makedirs(local_folder, exist_ok=True)
        
        files = list_files(hdfs, hdfs_src)
        
        if not files:
            print(f"    No files found in HDFS path: {hdfs_src}")
            continue
            
        counter = 0
        for original_filename in files:
            if original_filename.startswith("_") or original_filename.endswith(".crc"):
                continue

            ext = os.path.splitext(original_filename)[1]
            if not ext:
                ext = expected_ext
            
            
            if counter == 0:
                new_filename = f"{name}_{RUN_DATE}{ext}"
            else:
                new_filename = f"{name}_{RUN_DATE}_part{counter}{ext}"
            
            full_local_path = os.path.join(local_folder, new_filename)
            full_hdfs_path = f"{hdfs_src}/{original_filename}"
            
            print(f"   ⬇️  Downloading HDFS file: {original_filename}")
            print(f"   ✨ Saved locally as:      {new_filename}")
            
            try:
                hdfs.get_file(full_hdfs_path, full_local_path)
                counter += 1
            except Exception as e:
                print(f"   ❌ Failed: {e}")

    print(f"\n✅ DONE! Check your 'data/' folder structure.")

if __name__ == "__main__":
    main()
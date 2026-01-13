import json
import csv
from datetime import datetime
import os
import psycopg2 

class DataQualityGuard:
    def __init__(self, batch_date, db_config):
        self.batch_date = batch_date # Format expected: "YYYY-MM-DD"
        self.errors = []
        # Load limits immediately
        self.product_limits = self.load_product_limits(db_config)

    def load_product_limits(self, db_config):
        """Fetch MxOQ from Postgres."""
        print("🔌 Connecting to Postgres to fetch Product Rules...")
        try:
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()
            cur.execute("SELECT sku, mxoq FROM products;")
            results = cur.fetchall()
            conn.close()
            # Create dictionary: {'SKU-123': 500}
            return {row[0]: row[1] for row in results}
        except Exception as e:
            print(f"❌ Database Error: {e}")
            return {}

    def log_issue(self, rule_name, entity_id, details, severity="HIGH"):
        self.errors.append({
            "timestamp": datetime.now().isoformat(),
            "batch_date": self.batch_date,
            "rule_broken": rule_name,
            "entity_id": entity_id,
            "details": details,
            "severity": severity
        })

    # --- CHECKS ---
    def check_order_magnitude(self, order_id, sku, quantity):
        max_allowed = self.product_limits.get(sku)
        
        if max_allowed is None:
            self.log_issue("UNKNOWN_PRODUCT", sku, "SKU not found in Master Data.")
            return False

        if quantity > max_allowed:
            self.log_issue(
                "ABNORMAL_DEMAND_SPIKE", 
                order_id, 
                f"Qty {quantity} > Max {max_allowed}"
            )
            return False
        return True

    def check_stock_logic(self, sku, available, reserved):
        if reserved > available:
            self.log_issue("IMPOSSIBLE_STOCK", sku, f"Reserved {reserved} > Available {available}")
            return False
        return True

    # --- IMPROVED REPORT SAVING ---
    def save_report(self, base_output_dir = "data/hdfs/logs/exceptions/"):
        """
        Saves the report into a date-partitioned folder.
        Example: base_output_dir/date=2025-12-30/exceptions.csv
        """
        if not self.errors:
            print(" No exceptions to log.")
            return

        # 1. Construct the Hive-Style Partition Path
        # Output becomes: data/hdfs/logs/exceptions/date=2025-12-30/
        partition_dir = os.path.join(base_output_dir, f"date={self.batch_date}")
        os.makedirs(partition_dir, exist_ok=True)
        
        # 2. Define the full filename
        full_path = os.path.join(partition_dir, "exceptions.csv")

        # 3. Write CSV
        try:
            with open(full_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.errors[0].keys())
                writer.writeheader()
                writer.writerows(self.errors)
            print(f"⚠️  Exceptions Report saved: {full_path}")
        except Exception as e:
            print(f"❌ Failed to write log file: {e}")



# How we can use it:
# # Your Database Config (matches your docker-compose)
# db_config = {
#     "host": "localhost",
#     "port": "5432", 
#     "database": "procurement_db",
#     "user": "procurement_user",
#     "password": "procurement_pass"
# }

# # 1. Initialize
# guard = DataQualityGuard("2025-12-30", db_config)

# # 2. Do checks...
# guard.check_order_magnitude(...)

# # 3. Save (Just pass the root folder!)
# # The class will automatically add "/date=2025-12-30/exceptions.csv"
# guard.save_report("data/hdfs/logs/exceptions")
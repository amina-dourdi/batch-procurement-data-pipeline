import json
import csv
from datetime import datetime
import os
import psycopg2
from logger import log as logger

class DataQualityGuard:
    def __init__(self, batch_date, db_config):
        self.batch_date = batch_date  # Format: "YYYY-MM-DD"
        self.errors = []
        self.product_limits = self.load_product_limits(db_config)

    # --------------------------------------------------
    # MASTER DATA LOADING
    # --------------------------------------------------
    def load_product_limits(self, db_config):
        """Fetch MxOQ from Postgres."""
        logger.info("Connecting to Postgres to fetch Product Rules...")

        try:
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()

            cur.execute("SELECT sku, mxoq FROM products;")
            results = cur.fetchall()

            cur.close()
            conn.close()

            limits = {row[0]: row[1] for row in results}
            logger.info("Loaded %d product rules.", len(limits))
            return limits

        except Exception:
            logger.error("Database error while loading product limits", exc_info=True)
            return {}

    # --------------------------------------------------
    # EXCEPTION REGISTRY (BUSINESS LOG)
    # --------------------------------------------------
    def log_issue(self, rule_name, entity_id, details, severity="HIGH"):
        self.errors.append({
            "timestamp": datetime.now().isoformat(),
            "batch_date": self.batch_date,
            "rule_broken": rule_name,
            "entity_id": entity_id,
            "details": details,
            "severity": severity
        })

    # --------------------------------------------------
    # DATA QUALITY CHECKS
    # --------------------------------------------------
    def check_order_magnitude(self, order_id, sku, quantity):
        max_allowed = self.product_limits.get(sku)

        if max_allowed is None:
            self.log_issue("UNKNOWN_PRODUCT", sku, "SKU not found in Master Data.")
            logger.warning("Unknown SKU detected: %s", sku)
            return False

        if quantity > max_allowed:
            self.log_issue(
                "ABNORMAL_DEMAND_SPIKE",
                order_id,
                f"Qty {quantity} > Max {max_allowed}"
            )
            logger.warning(
                "Abnormal demand spike | Order %s | SKU %s | Qty %s > %s",
                order_id, sku, quantity, max_allowed
            )
            return False

        return True

    def check_stock_logic(self, sku, available, reserved):
        if reserved > available:
            self.log_issue(
                "IMPOSSIBLE_STOCK",
                sku,
                f"Reserved {reserved} > Available {available}"
            )
            logger.warning(
                "Impossible stock state | SKU %s | Reserved %s > Available %s",
                sku, reserved, available
            )
            return False

        return True

    # --------------------------------------------------
    # REPORT EXPORT (BUSINESS AUDIT)
    # --------------------------------------------------
    def save_report(self, base_output_dir="data/hdfs/logs/exceptions/"):
        """
        Saves the report into a date-partitioned folder.
        Example:
        data/hdfs/logs/exceptions/date=2026-01-13/exceptions.csv
        """

        if not self.errors:
            logger.info("No data-quality exceptions to save.")
            return

        # 1. Hive-style partition directory
        partition_dir = os.path.join(base_output_dir, f"date={self.batch_date}")
        os.makedirs(partition_dir, exist_ok=True)

        # 2. File path
        full_path = os.path.join(partition_dir, "exceptions.csv")

        # 3. Write CSV
        try:
            with open(full_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.errors[0].keys())
                writer.writeheader()
                writer.writerows(self.errors)

            logger.warning("Exceptions report saved: %s", full_path)

        except Exception:
            logger.error("Failed to write exceptions report", exc_info=True)

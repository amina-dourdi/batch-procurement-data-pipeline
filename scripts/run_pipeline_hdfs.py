import os
import pandas as pd
from datetime import datetime, date
from hdfs_client import WebHDFSClient

# --- IMPORT DES ÉTAPES ---
import aggregate_orders
import net_demand
import supplier_orders
from data_quality import DataQualityGuard  # On importe ta classe de qualité

# --- 1. CONFIGURATION ---
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")
HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

# Config Postgres pour le DataQualityGuard
DB_CONFIG = {
    "host": "localhost",
    "port": "5432", 
    "database": "procurement_db",
    "user": "procurement_user",
    "password": "procurement_pass"
}

def setup_hdfs_structure(hdfs):
    """Prépare les dossiers HDFS."""
    folders = [
        f"/processed/aggregated_orders/{RUN_DATE}",
        f"/processed/net_demand/{RUN_DATE}",
        f"/output/supplier_orders/{RUN_DATE}",
        f"/logs/exceptions/{RUN_DATE}"
    ]
    for folder in folders:
        hdfs.mkdirs(folder)

def main():
    hdfs = WebHDFSClient(HDFS_BASE_URL, user=HDFS_USER)
    
    # 1. Initialiser le garde-fou de qualité 
    guard = DataQualityGuard(RUN_DATE, DB_CONFIG)
    
    try:
        print(f"🚀 --- DÉMARRAGE DU PIPELINE GLOBAL ({RUN_DATE}) ---")
        
        # --- ÉTAPE 0 : PRÉPARATION & QUALITÉ DES FORMATS ---
        setup_hdfs_structure(hdfs)
        
        # Vérification des extensions (Whitelist)
        local_dir = os.path.join(DATA_ROOT, "raw", RUN_DATE)
        if os.path.exists(local_dir):
            for file_name in os.listdir(local_dir):
                ext = os.path.splitext(file_name)[1].lower()
                if ext not in {'.avro', '.csv', '.json', '.parquet'}:
                    guard.log_issue("INVALID_FORMAT", file_name, f"Extension {ext} interdite", "MEDIUM")

        # --- ÉTAPE 1 : AGGRÉGATION ---
        print("\n[Étape 1] Agrégation...")
        aggregate_orders.main()

        # --- ÉTAPE 2 : QUALITÉ MÉTIER (Magnitude & Stock) ---
        # Ici on peut appeler tes fonctions spécifiques si on a accès aux données
        # Exemple : guard.check_order_magnitude(order_id, sku, qty)
        # Exemple : guard.check_stock_logic(sku, available, reserved)

        # --- ÉTAPE 3 : DEMANDE NETTE ---
        print("\n[Étape 2] Calcul Demande Nette...")
        net_demand.main()

        # --- ÉTAPE 4 : COMMANDES FOURNISSEURS ---
        print("\n[Étape 3] Génération Commandes...")
        supplier_orders.main()

        # --- ÉTAPE FINALE : SAUVEGARDE DES LOGS ---
        # On utilise ta fonction save_report
        guard.save_report(os.path.join(DATA_ROOT, "logs/exceptions"))
        
        # Optionnel : Envoyer le rapport final de guard vers HDFS
        report_path = os.path.join(DATA_ROOT, f"logs/exceptions/date={RUN_DATE}/exceptions.csv")
        if os.path.exists(report_path):
            hdfs.put_file(report_path, f"/logs/exceptions/{RUN_DATE}/quality_report.csv", overwrite=True)

        print(f"\n✅ --- PIPELINE TERMINÉ ---")

    except Exception as e:
        print(f"\n🛑 ERREUR : {e}")
        guard.log_issue("CRITICAL_PIPELINE_FAILURE", "SYSTEM", str(e))
        guard.save_report(os.path.join(DATA_ROOT, "logs/exceptions"))

if __name__ == "__main__":
    main()
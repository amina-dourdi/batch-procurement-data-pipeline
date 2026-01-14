import os
import pandas as pd
from datetime import datetime, date
import fastavro
from trino.dbapi import connect 
from hdfs_client import WebHDFSClient
import requests
# --- IMPORT DES ÉTAPES ---
import generate_daily_files
import aggregate_orders
import net_demand
import supplier_orders
from data_quality import DataQualityGuard  # Import de votre garde-fou
# from trino_utils import ensure_schema

# --- 1. CONFIGURATION ---
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")
HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

TRINO_HOST = os.environ["TRINO_HOST"]
TRINO_PORT = int(os.getenv("TRINO_PORT", 8080))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "hive")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "default")


# Configuration pour la connexion Postgres (utilisée par DataQualityGuard)
DB_CONFIG = {
    "host": "postgres",
    "port": "5432", 
    "database": "procurement_db",
    "user": "procurement_user",
    "password": "procurement_pass"
}

def setup_hdfs_structure(hdfs):
    """Crée l'arborescence complète demandée dans HDFS."""
    folders = [
        f"/raw/orders/{RUN_DATE}",
        f"/raw/stock/{RUN_DATE}",
        f"/processed/aggregated_orders/{RUN_DATE}",
        f"/processed/net_demand/{RUN_DATE}",
        f"/output/supplier_orders/{RUN_DATE}",
        f"/logs/exceptions/date={RUN_DATE}"
    ]
    for folder in folders:
        print(f" Configuration HDFS : {folder}")
        hdfs.mkdirs(folder)


def check_files_existence():
    """A simple check to ensure the files were generated locally."""
    local_dir = os.path.join(DATA_ROOT, "raw/orders", RUN_DATE)
    if not os.path.exists(local_dir):
        print(f" Warning: Local directory not found: {local_dir}")
        return
    
    files = [f for f in os.listdir(local_dir) if f.endswith('.avro')]
    print(f"  Found {len(files)} Avro files ready for processing.")

def main():
    hdfs = WebHDFSClient(HDFS_BASE_URL, user=HDFS_USER)
    
    # ensure_schema("processed")

    # 1. Initialisation du Garde (Charge les MxOQ depuis Postgres)
    guard = DataQualityGuard(RUN_DATE, DB_CONFIG)
    
    try:
        print(f"\n --- DÉMARRAGE DU PIPELINE GLOBAL ({RUN_DATE}) ---")
        
        # 1. Connect to Trino (Service Name: trino)
        
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA
        )    
        cur = conn.cursor()


        # --- 🛠️ FIX: CREATE SCHEMAS FIRST ---
        # We must ensure the 'folders' exist in the database before creating tables in them.
        print("Checking schemas...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS default")
        cur.execute("CREATE SCHEMA IF NOT EXISTS processed")
        cur.execute("CREATE SCHEMA IF NOT EXISTS hive.output")
        
        # --- ÉTAPE 0 : PRÉPARATION, GÉNÉRATION ET VALIDATION ---
        print("\n[Étape 0] Préparation HDFS et Simulation Chaos...")
        setup_hdfs_structure(hdfs)
        
        # Génération des fichiers (avec erreurs simulées)
        generate_daily_files.main()
        
        check_files_existence()

        # --- ÉTAPE 1 : AGGRÉGATION (Trino) ---
        print("\n[Étape 1] Lancement de l'agrégation des ventes...")
        # On passe le guard pour vérifier la Magnitude (MxOQ)
        aggregate_orders.main(guard)

        # --- ÉTAPE 2 : DEMANDE NETTE (Trino) ---
        print("\n[Étape 2] Lancement du calcul de la demande nette...")
        # On passe le guard pour vérifier la Logique de Stock (Reserved > Available)
        net_demand.main(guard)

        # --- ÉTAPE 3 : COMMANDES FOURNISSEURS (Trino) ---
        print("\n[Étape 3] Génération des ordres d'achat...")
        supplier_orders.main(guard)

        # --- ÉTAPE FINALE : SAUVEGARDE ET EXPORT DU RAPPORT ---
        print("\n[Étape 4] Sauvegarde du rapport d'exceptions...")
        log_dir_local = os.path.join(DATA_ROOT, "logs/exceptions")
        
        # Sauvegarde le CSV localement (gère la création du dossier date=...)
        guard.save_report(log_dir_local)
        
        # Copie du rapport vers HDFS pour archivage centralisé
        local_report_file = os.path.join(log_dir_local, f"date={RUN_DATE}/exceptions.csv")
        if os.path.exists(local_report_file):
            hdfs.put_file(local_report_file, f"/logs/exceptions/date={RUN_DATE}/exceptions.csv", overwrite=True)

        print(f"\n --- PIPELINE TERMINÉ AVEC SUCCÈS POUR LE {RUN_DATE} ---")

    except Exception as e:
        print(f"\n ERREUR CRITIQUE DANS LE PIPELINE : {e}")
        guard.log_issue("PIPELINE_CRASH", "SYSTEM", str(e))
        guard.save_report(os.path.join(DATA_ROOT, "logs/exceptions"))

if __name__ == "__main__":
    main()
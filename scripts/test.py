# scripts/run_pipeline_hdfs.py

import os
from datetime import date
from hdfs_client import WebHDFSClient

# --- IMPORT DES ÉTAPES ---
import generate_daily_files
import aggregate_orders
import net_demand
import supplier_orders
from data_quality import DataQualityGuard

# --- 1. CONFIGURATION ---
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")

HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

# IMPORTANT (Docker) : utiliser les noms de services
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

PG_HOST = os.getenv("PG_HOST", "postgres")  # <- pas localhost dans orchestrator
DB_CONFIG = {
    "host": PG_HOST,
    "port": os.getenv("PG_PORT", "5432"),
    "database": os.getenv("PG_DB", "procurement_db"),
    "user": os.getenv("PG_USER", "procurement_user"),
    "password": os.getenv("PG_PASS", "procurement_pass"),
}


# -----------------------------
# Helpers: Local validations
# -----------------------------
def validate_files_and_log_errors(guard: DataQualityGuard):
    """Vérifie la validité des fichiers RAW locaux (orders) et log les anomalies."""
    local_dir = os.path.join(DATA_ROOT, "raw", "orders", RUN_DATE)

    if not os.path.exists(local_dir):
        print(f" Aucun dossier local trouvé pour la date : {local_dir}")
        return

    allowed = {".avro", ".csv", ".json", ".parquet"}

    for file_name in os.listdir(local_dir):
        ext = os.path.splitext(file_name)[1].lower()
        if ext not in allowed:
            guard.log_issue(
                rule_name="INVALID_FORMAT",
                entity_id=file_name,
                details=f"Format {ext} non supporté",
                severity="MEDIUM",
            )


# -----------------------------
# Helpers: HDFS structure
# -----------------------------
def setup_hdfs_structure(hdfs: WebHDFSClient):
    """
    Crée seulement les dossiers "parents" pour éviter le blocage Trino (HIVE_PATH_ALREADY_EXISTS).
    Les dossiers datés /processed/.../{RUN_DATE} seront créés par Trino au moment du CREATE TABLE.
    """
    folders = [
        f"/raw/orders/{RUN_DATE}",
        f"/raw/stock/{RUN_DATE}",
        f"/logs/exceptions/date={RUN_DATE}",
        "/processed/aggregated_orders",
        "/processed/net_demand",
        "/output/supplier_orders",
    ]
    for folder in folders:
        print(f" Configuration HDFS : {folder}")
        hdfs.mkdirs(folder)


def setup_local_structure():
    """Crée les dossiers locaux miroir (dans /app/data) pour la visibilité."""
    folders = [
        os.path.join(DATA_ROOT, "raw", "orders", RUN_DATE),
        os.path.join(DATA_ROOT, "raw", "stock", RUN_DATE),
        os.path.join(DATA_ROOT, "raw", "processed", "aggregated_orders", RUN_DATE),
        os.path.join(DATA_ROOT, "raw", "processed", "net_demand", RUN_DATE),
        os.path.join(DATA_ROOT, "raw", "output", "supplier_orders", RUN_DATE),
    ]
    for f in folders:
        os.makedirs(f, exist_ok=True)


def cleanup_hdfs_date_dirs(hdfs: WebHDFSClient):
    """
    Nettoyage des répertoires datés avant de relancer le pipeline.
    Utile si tu relances RUN_DATE et que Trino se plaint que le dossier existe déjà.
    """
    paths = [
        f"/processed/aggregated_orders/{RUN_DATE}",
        f"/processed/net_demand/{RUN_DATE}",
        f"/output/supplier_orders/{RUN_DATE}",
    ]
    for p in paths:
        if hdfs.exists(p):
            print(f"🧹 Suppression HDFS existant: {p}")
            hdfs.delete(p, recursive=True)


# -----------------------------
# Helpers: HDFS listing/copy
# -----------------------------
# def _list_files_in_hdfs_dir(hdfs: WebHDFSClient, hdfs_dir: str):
#     """
#     Retourne la liste des noms de fichiers dans un dossier HDFS (pas les sous-dossiers).
#     Compatible avec WebHDFS style responses.
#     """
#     if not hdfs.exists(hdfs_dir):
#         return []

#     st = hdfs.list(hdfs_dir)
#     items = st.get("FileStatuses", {}).get("FileStatus", []) if isinstance(st, dict) else st

#     files = []
#     for it in items or []:
#         if it.get("type") == "FILE" and it.get("pathSuffix"):
#             files.append(it["pathSuffix"])
#     return files
def _list_files_in_hdfs_dir(hdfs, hdfs_dir: str):
    """
    Returns the list of file names in an HDFS directory (no sub-directories).
    Compatible with common WebHDFS Python clients.
    """
    try:
        if not hdfs.exists(hdfs_dir):
            return []

        items = hdfs.list(hdfs_dir)   # ✅ correct method

        files = []
        for name in items:
            full_path = f"{hdfs_dir}/{name}"
            status = hdfs.status(full_path)

            if status.get("type") == "FILE":
                files.append(name)

        return files

    except Exception as e:
        print(f"❌ Error listing HDFS directory {hdfs_dir}: {e}")
        return []


def mirror_hdfs_dir_to_local(hdfs: WebHDFSClient, hdfs_dir: str, local_dir: str):
    """
    Copie tous les fichiers d’un dossier HDFS vers un dossier local.
    IMPORTANT: on ne copie QUE si le dossier existe ET contient au moins 1 fichier.
    """
    files = _list_files_in_hdfs_dir(hdfs, hdfs_dir)

    if not hdfs.exists(hdfs_dir):
        print(f"❌ HDFS: dossier inexistant (Trino n'a rien créé): {hdfs_dir}")
        return

    if len(files) == 0:
        print(f"⚠️ HDFS: dossier موجود لكن فارغ (0 fichier): {hdfs_dir}")
        print("   ➜ Ça veut dire que Trino n'a pas écrit de sortie (erreur ou résultat vide).")
        return

    os.makedirs(local_dir, exist_ok=True)

    nb = 0
    for name in files:
        hdfs_file = f"{hdfs_dir.rstrip('/')}/{name}"
        local_file = os.path.join(local_dir, name)

        # IMPORTANT: pas de overwrite=True (selon ton client)
        hdfs.get_file(hdfs_file, local_file)
        nb += 1
        print(f"✅ Mirror: {hdfs_file} -> {local_file}")

    print(f"📦 {nb} fichier(s) copiés depuis {hdfs_dir} vers {local_dir}")


# -----------------------------
# Patch Trino connect (Docker)
# -----------------------------
def patch_trino_connect_in_modules():
    """
    Sans modifier aggregate_orders.py / net_demand.py :
    on remplace la fonction `connect` importée chez eux, pour pointer vers le service Docker 'trino'
    au lieu de 127.0.0.1.
    """
    import trino.dbapi

    def _connect(*args, **kwargs):
        kwargs.pop("host", None)
        kwargs.pop("port", None)
        return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, *args, **kwargs)

    # ils font: from trino.dbapi import connect
    # donc chez eux `connect` est une variable => on l'écrase.
    aggregate_orders.connect = _connect
    net_demand.connect = _connect


# -----------------------------
# Main pipeline
# -----------------------------
def main():
    hdfs = WebHDFSClient(HDFS_BASE_URL, user=HDFS_USER)

    # Patch Trino (Docker)
    patch_trino_connect_in_modules()

    # Initialisation du Guard (Postgres via service Docker)
    guard = DataQualityGuard(RUN_DATE, DB_CONFIG)

    try:
        print(f"\n --- DÉMARRAGE DU PIPELINE GLOBAL ({RUN_DATE}) ---")

        print("\n[Étape 0] Préparation HDFS + Local...")
        setup_hdfs_structure(hdfs)
        setup_local_structure()
        cleanup_hdfs_date_dirs(hdfs)

        print("\n[Étape 0b] Génération des fichiers RAW...")
        generate_daily_files.main()

        print("\n[Étape 0c] Validation formats...")
        validate_files_and_log_errors(guard)

        print("\n[Étape 1] Agrégation des ventes (Trino)...")
        aggregate_orders.main(guard)
        mirror_hdfs_dir_to_local(
            hdfs,
            f"/processed/aggregated_orders/{RUN_DATE}",
            os.path.join(DATA_ROOT, "raw", "processed", "aggregated_orders", RUN_DATE),
        )

        print("\n[Étape 2] Calcul Net Demand (Trino)...")
        net_demand.main(guard)
        mirror_hdfs_dir_to_local(
            hdfs,
            f"/processed/net_demand/{RUN_DATE}",
            os.path.join(DATA_ROOT, "raw", "processed", "net_demand", RUN_DATE),
        )

        print("\n[Étape 3] Supplier Orders (Trino)...")
        supplier_orders.main()
        mirror_hdfs_dir_to_local(
            hdfs,
            f"/output/supplier_orders/{RUN_DATE}",
            os.path.join(DATA_ROOT, "raw", "output", "supplier_orders", RUN_DATE),
        )

        print("\n[Étape 4] Sauvegarde du rapport d'exceptions...")
        log_dir_local = os.path.join(DATA_ROOT, "logs", "exceptions")
        guard.save_report(log_dir_local)

        local_report_file = os.path.join(log_dir_local, f"date={RUN_DATE}", "exceptions.csv")
        if os.path.exists(local_report_file):
            hdfs.put_file(
                local_report_file,
                f"/logs/exceptions/date={RUN_DATE}/exceptions.csv",
                overwrite=True,
            )

        print(f"\n --- PIPELINE TERMINÉ AVEC SUCCÈS POUR LE {RUN_DATE} ---")

    except Exception as e:
        print(f"\n ERREUR CRITIQUE DANS LE PIPELINE : {e}")
        guard.log_issue("PIPELINE_CRASH", "SYSTEM", str(e))
        guard.save_report(os.path.join(DATA_ROOT, "logs", "exceptions"))


if __name__ == "__main__":
    main()

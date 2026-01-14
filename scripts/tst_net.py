import os
import sys
from net_demand import main
from data_quality import DataQualityGuard

# 1. CONFIGURATION DE L'ENVIRONNEMENT
# On définit la date de travail
RUN_DATE = "2026-01-13" 
os.environ["RUN_DATE"] = RUN_DATE

# Configuration Postgres (Utilisation de 127.0.0.1 pour éviter l'erreur IPv6 localhost)
DB_CONFIG = {
    "host": "127.0.0.1", 
    "port": "5432", 
    "database": "procurement_db",
    "user": "procurement_user",
    "password": "procurement_pass"
}

def test_creation():
    print(f"🧪 DÉMARRAGE DU TEST ISOLÉ : Net Demand ({RUN_DATE})")
    print("-" * 50)
    
    # 2. INITIALISATION DU GARDE-FOU
    # Cette étape va tenter de se connecter à Postgres pour charger le MxOQ
    try:
        guard = DataQualityGuard(RUN_DATE, DB_CONFIG)
        print("✅ DataQualityGuard initialisé et règles chargées.")
    except Exception as e:
        print(f"❌ ÉCHEC de l'initialisation du Guard (Postgres) : {e}")
        return

    # 3. APPEL DU TRAITEMENT TRINO
    try:
        print(f"🔄 Exécution du calcul Trino pour la date {RUN_DATE}...")
        main(guard)
        print("✅ La requête Trino (Calcul Net Demand) a été exécutée.")
        
        # 4. AFFICHAGE DES RÉSULTATS DE QUALITÉ
        print("-" * 50)
        if guard.errors:
            # On filtre pour n'afficher que les anomalies de stock logiques
            stock_errors = [e for e in guard.errors if e['rule_broken'] == 'IMPOSSIBLE_STOCK']
            
            if stock_errors:
                print(f"⚠️ {len(stock_errors)} anomalies de stock détectées :")
                for err in stock_errors:
                    print(f"   - SKU: {err['entity_id']} | {err['details']}")
            else:
                print("✨ Aucune anomalie de logique de stock (Reserved > Available) détectée.")
        else:
            print("✨ Aucune erreur de qualité signalée par le Guard.")

    except Exception as e:
        print(f"❌ LE TEST TRINO A ÉCHOUÉ :")
        print(f"   Détail : {e}")

if __name__ == "__main__":
    test_creation()
import os
from net_demand import main
from data_quality import DataQualityGuard

# 1. Configuration manuelle pour le test
RUN_DATE = "2026-01-13"  # Assure-toi que des données existent pour cette date
os.environ["RUN_DATE"] = RUN_DATE

DB_CONFIG = {
    "host": "localhost",
    "port": "5432", 
    "database": "procurement_db",
    "user": "procurement_user",
    "password": "procurement_pass"
}

def test_creation():
    print(f"🧪 Démarrage du test isolé pour Net Demand ({RUN_DATE})")
    
    # 2. Initialisation du garde-fou pour voir s'il capture les erreurs de stock
    guard = DataQualityGuard(RUN_DATE, DB_CONFIG)
    
    try:
        # 3. Appel de ta fonction main de net_demand.py
        main(guard)
        
        print("✅ La requête Trino a été exécutée avec succès.")
        
        # 4. Affichage des erreurs de stock trouvées (s'il y en a)
        if guard.errors:
            print(f"⚠️ {len(guard.errors)} anomalies de stock détectées pendant le test :")
            for err in guard.errors:
                if err['rule_broken'] == 'IMPOSSIBLE_STOCK':
                    print(f"   - SKU: {err['entity_id']} | Détails: {err['details']}")
        else:
            print("✨ Aucune anomalie de logique de stock trouvée.")

    except Exception as e:
        print(f"❌ Le test a échoué : {e}")

if __name__ == "__main__":
    test_creation()
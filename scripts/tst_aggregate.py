import os
from aggregate_orders import main  # adapte si ton fichier s'appelle aggregate_order.py

# 1) CONFIGURATION DE TEST
RUN_DATE = "2026-01-13"
os.environ["RUN_DATE"] = RUN_DATE

def test_aggregation():
    print(f"🧪 DÉMARRAGE DU TEST : Agrégation des Commandes ({RUN_DATE})")
    print("-" * 60)

    # ✅ On désactive le Guard pour ne pas dépendre de Postgres (qui crash chez toi)
    guard = None

    # 2) EXÉCUTION DE L'AGRÉGATION
    try:
        print("🔄 Trino : Lecture des Avro et création de la table agrégée...")
        main(guard)
        print(f"✅ Succès : La table aggregated_orders_{RUN_DATE.replace('-', '_')} a été créée.")
        print("✨ Test terminé sans DataQualityGuard (Postgres ignoré).")

    except Exception as e:
        print("\n❌ ÉCHEC DU TEST :")
        print(f"Détail de l'erreur : {e}")

if __name__ == "__main__":
    test_aggregation()

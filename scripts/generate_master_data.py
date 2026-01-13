import pandas as pd
import random
from faker import Faker
import os

# Initialize Faker
fake = Faker()
# specific seed for reproducibility (so you get the same data every time you run it)
Faker.seed(42) 

# --- CONFIGURATION ---
NUM_SUPPLIERS = 20
NUM_MARKETS = 10
NUM_PRODUCTS = 300
OUTPUT_DIR = "data/postgres_load"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"🚀 Starting Data Generation in '{OUTPUT_DIR}'...")

# ==========================================
# 1. GENERATE SUPPLIERS
# ==========================================
print("--- Generating Suppliers ---")
suppliers = []
# Create IDs like SUP-001, SUP-002...
supplier_ids = [f"SUP-{str(i).zfill(3)}" for i in range(1, NUM_SUPPLIERS + 1)]

for s_id in supplier_ids:
    suppliers.append({
        "supplier_id": s_id,
        "name": fake.company(),
        "country": fake.country(),
        "contact_email": fake.company_email(),
        "location": fake.city()  # Added per your request
    })

df_suppliers = pd.DataFrame(suppliers)
df_suppliers.to_csv(f"{OUTPUT_DIR}/suppliers.csv", index=False)
print(f"✔ Created {len(df_suppliers)} suppliers.")

# ==========================================
# 2. GENERATE MARKETS (Stores)
# ==========================================
print("--- Generating Markets ---")
markets = []
# Create IDs like MKT-001...
market_ids = [f"MKT-{str(i).zfill(3)}" for i in range(1, NUM_MARKETS + 1)]

for m_id in market_ids:
    markets.append({
        "market_id": m_id,
        "location": fake.address().replace("\n", ", "), # Full address
        "type": random.choice(["Superstore", "Express", "Click & Collect"])
    })

df_markets = pd.DataFrame(markets)
df_markets.to_csv(f"{OUTPUT_DIR}/market.csv", index=False)
print(f"✔ Created {len(df_markets)} markets.")

# ==========================================
# 3. GENERATE PRODUCTS
# ==========================================
print("--- Generating Products ---")
products = []
categories = ['Dairy', 'Bakery', 'Canned Goods', 'Beverages', 'Cleaning', 'Produce', 'Meat']
package_types = ['Box of 6', 'Box of 12', 'Box of 24', 'Single Unit', 'Pallet']

# Create SKUs like SKU-0001...
sku_list = [f"SKU-{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]

for sku in sku_list:
    # Critical: Pick a supplier_id that actually exists!
    assigned_supplier = random.choice(supplier_ids)
    
    # Logic for MOQ and MxOQ
    moq = random.choice([10, 50, 100])
    mxoq = moq * random.randint(5, 20) # Max is always greater than Min
    
    products.append({
        "sku": sku,
        "name": f"Product {fake.word().capitalize()} {fake.random_int(1,100)}",
        "category": random.choice(categories),
        "unit_price": round(random.uniform(2.50, 150.00), 2),
        "supplier_id": assigned_supplier, # Matches the foreign key
        "MOQ": moq,
        "MxOQ": mxoq,
        "package": random.choice(package_types),
        "leadtime": random.randint(1, 14) # 1 to 14 days delivery time
    })

df_products = pd.DataFrame(products)
df_products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
print(f"✔ Created {len(df_products)} products.")

print("\n✨ Generation Complete. CSV files are ready to be loaded into PostgreSQL.")
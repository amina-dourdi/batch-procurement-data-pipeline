# import pandas as pd
# import os

# DATA_ROOT = "/app/data"  # or your DATA_ROOT
# RUN_DATE = "2026-01-13"  # example date

# # Paths
# local_orders = os.path.join(DATA_ROOT, "raw/orders/", RUN_DATE, "orders_MKT-001.avro")
# local_stock  = os.path.join(DATA_ROOT, "raw/stock", RUN_DATE, "stock.avro")

# # Read
# df_orders = pd.read_parquet(local_orders)
# df_stock = pd.read_parquet(local_stock)

# # Show data
# print(df_orders.head())
# print(df_stock.head())
import pandas as pd
import pandavro as pdx  
import os

DATA_ROOT = "/app/data"
RUN_DATE = "2026-01-13" 

orders_dir = os.path.join(DATA_ROOT, "raw/orders", RUN_DATE)
stock_dir  = os.path.join(DATA_ROOT, "raw/stock", RUN_DATE)

print(f"--- 📂 Reading from: {orders_dir} ---\n")

# -------------------------------------------------------
# 1. قراءة ملف ORDERS (الطلبات)
# -------------------------------------------------------
# بما أن أسواقاً قد تغيب عشوائياً، سنبحث عن أول ملف موجود
try:
    # نأخذ قائمة الملفات في المجلد
    files = [f for f in os.listdir(orders_dir) if f.endswith('.avro')]
    
    if files:
        first_file = files[0] # نختار أول ملف نجده
        full_path = os.path.join(orders_dir, first_file)
        
        print(f"📖 Reading File: {first_file}")
        
        # الأمر السحري لقراءة Avro
        df_orders = pdx.read_avro(full_path)
        
        # عرض البيانات
        print(f"   Shape: {df_orders.shape} (Rows, Columns)")
        print("\n--- Content (First 5 Rows) ---")
        print(df_orders.head())
        print("-" * 50)
    else:
        print("❌ No Avro files found in Orders folder.")

except FileNotFoundError:
    print(f"❌ Directory not found: {orders_dir}")


# -------------------------------------------------------
# 2. قراءة ملف STOCK (المخزون)
# -------------------------------------------------------
stock_file = os.path.join(stock_dir, "stock.avro")

print(f"\n--- 📦 Reading Stock File ---")
if os.path.exists(stock_file):
    df_stock = pdx.read_avro(stock_file)
    
    print(f"   Shape: {df_stock.shape}")
    print("\n--- Content (First 5 Rows) ---")
    print(df_stock.head())
else:
    print(f"❌ Stock file not found at: {stock_file}")
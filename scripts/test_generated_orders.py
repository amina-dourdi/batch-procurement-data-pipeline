import pandas as pd
import os

DATA_ROOT = "/app/data"  # or your DATA_ROOT
RUN_DATE = "2026-01-13"  # example date

# Paths
local_orders = os.path.join(DATA_ROOT, "raw", RUN_DATE, "orders_market_MKT-001.parquet")
local_stock  = os.path.join(DATA_ROOT, "raw", RUN_DATE, "stock.parquet")

# Read
df_orders = pd.read_parquet(local_orders)
df_stock = pd.read_parquet(local_stock)

# Show data
print(df_orders.head())
print(df_stock.head())

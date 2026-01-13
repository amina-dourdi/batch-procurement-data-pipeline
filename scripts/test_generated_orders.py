import pandas as pd
import os

DATA_ROOT = "/app/data"  # or your DATA_ROOT
RUN_DATE = "2026-01-06"  # example date

# Paths
local_orders = os.path.join(DATA_ROOT, "tmp", RUN_DATE, "orders.parquet")
local_stock  = os.path.join(DATA_ROOT, "tmp", RUN_DATE, "stock.parquet")

# Read
df_orders = pd.read_parquet(local_orders)

# Show data
print(df_orders.head())

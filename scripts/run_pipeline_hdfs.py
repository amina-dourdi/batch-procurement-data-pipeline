import os
import math
import pandas as pd
from datetime import date
from hdfs_client import WebHDFSClient
from pg_client import read_sql_df

DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()

HDFS_BASE_URL = os.getenv("HDFS_BASE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

def pack_size_from_package(pkg: str) -> int:
    if not isinstance(pkg, str):
        return 1
    p = pkg.lower()
    if "box of" in p:
        digits = "".join([c for c in p if c.isdigit()])
        return int(digits) if digits else 1
    if "single" in p:
        return 1
    if "pallet" in p:
        return 100
    return 1

def round_up_to_pack(qty: int, pack: int) -> int:
    if pack <= 1:
        return qty
    return int(math.ceil(qty / pack) * pack)

def main():
    hdfs = WebHDFSClient(HDFS_BASE_URL, user=HDFS_USER)

    hdfs_orders = f"/raw/orders/{RUN_DATE}/orders.parquet"
    hdfs_stock  = f"/raw/stock/{RUN_DATE}/stock.parquet"
    if not hdfs.exists(hdfs_orders) or not hdfs.exists(hdfs_stock):
        raise FileNotFoundError("RAW manquant dans HDFS pour cette date.")

    # download raw local temp
    local_dir = os.path.join(DATA_ROOT, "tmp", RUN_DATE)
    os.makedirs(local_dir, exist_ok=True)
    local_orders = os.path.join(local_dir, "orders.parquet")
    local_stock  = os.path.join(local_dir, "stock.parquet")
    hdfs.get_file(hdfs_orders, local_orders)
    hdfs.get_file(hdfs_stock, local_stock)

    df_orders = pd.read_parquet(local_orders)
    df_stock  = pd.read_parquet(local_stock)

    # 1) aggregated_orders
    df_agg = (
        df_orders.groupby("sku", as_index=False)["quantity_sold"]
        .sum()
        .rename(columns={"quantity_sold": "quantity_sold_total"})
    )
    df_agg["run_date"] = RUN_DATE

    # 2) stock usable
    df_stock["free_stock"] = (df_stock["quantity_available"] - df_stock["quantity_reserved"]).clip(lower=0)
    df_stock_sku = df_stock.groupby("sku", as_index=False).agg(
        {"free_stock": "sum", "safety_quantity": "max"}
    )

    # 3) net demand
    df_net = df_agg.merge(df_stock_sku, on="sku", how="left")
    df_net["free_stock"] = df_net["free_stock"].fillna(0).astype(int)
    df_net["safety_quantity"] = df_net["safety_quantity"].fillna(0).astype(int)
    df_net["net_demand"] = (
        df_net["quantity_sold_total"] + df_net["safety_quantity"] - df_net["free_stock"]
    ).clip(lower=0).astype(int)

    # 4) supplier orders depuis Postgres (products table)
    df_products = read_sql_df('SELECT sku, supplier_id, "moq", package FROM products;')
    df_products["pack_size"] = df_products["package"].apply(pack_size_from_package)
    df_products["moq"] = df_products["moq"].fillna(0).astype(int)

    df_so = df_net.merge(df_products[["sku", "supplier_id", "moq", "pack_size"]], on="sku", how="left")
    df_so = df_so.dropna(subset=["supplier_id"]).copy()
    df_so = df_so[df_so["net_demand"] > 0].copy()

    df_so["quantity"] = df_so.apply(
        lambda r: round_up_to_pack(max(int(r["net_demand"]), int(r["moq"])), int(r["pack_size"])),
        axis=1
    )

    df_so_out = df_so[["run_date", "supplier_id", "sku", "quantity"]].copy()

    # save local processed/output
    local_agg = os.path.join(local_dir, "aggregated_orders.parquet")
    local_net = os.path.join(local_dir, "net_demand.parquet")
    local_so  = os.path.join(local_dir, "supplier_orders.parquet")

    df_agg.to_parquet(local_agg, index=False)
    df_net[["run_date", "sku", "net_demand"]].to_parquet(local_net, index=False)
    df_so_out.to_parquet(local_so, index=False)

    # upload to HDFS (write-once)
    hdfs_agg_dir = f"/processed/aggregated_orders/{RUN_DATE}"
    hdfs_net_dir = f"/processed/net_demand/{RUN_DATE}"
    hdfs_out_dir = f"/output/supplier_orders/{RUN_DATE}"
    hdfs.mkdirs(hdfs_agg_dir)
    hdfs.mkdirs(hdfs_net_dir)
    hdfs.mkdirs(hdfs_out_dir)

    hdfs_agg = f"{hdfs_agg_dir}/aggregated_orders.parquet"
    hdfs_net = f"{hdfs_net_dir}/net_demand.parquet"
    hdfs_so  = f"{hdfs_out_dir}/supplier_orders.parquet"

    if hdfs.exists(hdfs_agg) or hdfs.exists(hdfs_net) or hdfs.exists(hdfs_so):
        raise RuntimeError("Processed/output déjà existants (write-once).")

    hdfs.put_file(local_agg, hdfs_agg, overwrite=False)
    hdfs.put_file(local_net, hdfs_net, overwrite=False)
    hdfs.put_file(local_so,  hdfs_so,  overwrite=False)

    print("[OK] HDFS aggregated_orders ->", hdfs_agg)
    print("[OK] HDFS net_demand       ->", hdfs_net)
    print("[OK] HDFS supplier_orders  ->", hdfs_so)

if __name__ == "__main__":
    main()

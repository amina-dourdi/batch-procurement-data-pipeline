import os
from datetime import date
from trino.dbapi import connect  # pip install trino
from dotenv import load_dotenv

load_dotenv()

# Configuration
RUN_DATE = os.getenv("RUN_DATE") or date.today().isoformat()
TRINO_HOST = os.getenv("PRESTO_HOST", "trino")
TRINO_PORT = int(os.getenv("PRESTO_PORT", "8080"))
TRINO_USER = "admin"

def get_trino_connection():
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="hive",
        schema="default",
    )

def run_query(cursor, sql):
    print(f"Executing SQL: {sql[:50]}...")
    cursor.execute(sql)
    # Fetch result if it's a SELECT, otherwise just finish
    if cursor.description:
        return cursor.fetchall()
    return []

def main():
    print(f"---  STARTING TRINO PIPELINE FOR {RUN_DATE} ---")
    conn = get_trino_connection()
    cur = conn.cursor()

    # ==============================================================================
    # STEP 1: DEFINE EXTERNAL TABLES (Map HDFS files to SQL)
    # ==============================================================================
    # We drop them first to ensure we point to the fresh daily data
    
    # 1.1 Map the Raw Orders for THIS DATE
    cur.execute("DROP TABLE IF EXISTS raw_orders_daily")
    cur.execute(f"""
        CREATE TABLE raw_orders_daily (
            run_date VARCHAR,
            market_id VARCHAR,
            sku VARCHAR,
            quantity_sold INT,
            supplier_id VARCHAR
        )
        WITH (
            external_location = 'hdfs://namenode:9000/raw/orders/{RUN_DATE}',
            format = 'PARQUET'
        )
    """)

    # 1.2 Map the Raw Stock for THIS DATE
    cur.execute("DROP TABLE IF EXISTS raw_stock_daily")
    cur.execute(f"""
        CREATE TABLE raw_stock_daily (
            run_date VARCHAR,
            sku VARCHAR,
            quantity_available INT,
            quantity_reserved INT,
            safety_quantity INT,
            location VARCHAR
        )
        WITH (
            external_location = 'hdfs://namenode:9000/raw/stock/{RUN_DATE}',
            format = 'PARQUET'
        )
    """)

    # ==============================================================================
    # STEP 2: RUN THE LOGIC (Aggregation + Join Postgres + Calculation)
    # ==============================================================================
    # We use CTAS (Create Table As Select) to save the result directly to HDFS
    
    output_table = f"supplier_orders_{RUN_DATE.replace('-', '_')}"
    cur.execute(f"DROP TABLE IF EXISTS {output_table}")

    print(" Calculating Net Demand & Rounding (Distributed Join)...")
    
    sql_logic = f"""
    CREATE TABLE {output_table} 
    WITH (
        format = 'PARQUET',
        external_location = 'hdfs://namenode:9000/output/supplier_orders/{RUN_DATE}'
    ) AS
    WITH 
    -- A. Aggregate Sales per SKU
    agg_sales AS (
        SELECT sku, SUM(quantity_sold) as total_sold
        FROM raw_orders_daily
        GROUP BY sku
    ),
    -- B. Aggregate Stock (Global View)
    agg_stock AS (
        SELECT 
            sku, 
            SUM(GREATEST(quantity_available - quantity_reserved, 0)) as free_stock,
            MAX(safety_quantity) as safety_stock
        FROM raw_stock_daily
        GROUP BY sku
    ),
    -- C. Calculate Net Demand
    demand_calc AS (
        SELECT 
            COALESCE(s.sku, st.sku) as sku,
            COALESCE(s.total_sold, 0) as sold,
            COALESCE(st.free_stock, 0) as free_stock,
            COALESCE(st.safety_stock, 0) as safety,
            -- The Formula: Need = (Sold + Safety) - FreeStock
            GREATEST(
                (COALESCE(s.total_sold, 0) + COALESCE(st.safety_stock, 0)) - COALESCE(st.free_stock, 0), 
                0
            ) as net_demand
        FROM agg_sales s
        FULL OUTER JOIN agg_stock st ON s.sku = st.sku
    )
    -- D. Final Join with Postgres (Federated Query!) to apply MOQ & Pack Size
    SELECT 
        '{RUN_DATE}' as run_date,
        p.supplier_id,
        d.sku,
        -- The Python "round_up_to_pack" logic converted to SQL:
        -- Quantity = CEIL( MAX(NetDemand, MOQ) / PackSize ) * PackSize
        CAST(
            CEILING(
                CAST(GREATEST(d.net_demand, COALESCE(p.moq, 1)) AS DOUBLE) 
                / 
                CASE 
                    WHEN p.package LIKE '%Box of 6%' THEN 6
                    WHEN p.package LIKE '%Box of 12%' THEN 12
                    WHEN p.package LIKE '%Box of 24%' THEN 24
                    WHEN p.package LIKE '%Pallet%' THEN 100
                    ELSE 1 
                END
            ) * CASE 
                WHEN p.package LIKE '%Box of 6%' THEN 6
                WHEN p.package LIKE '%Box of 12%' THEN 12
                WHEN p.package LIKE '%Box of 24%' THEN 24
                WHEN p.package LIKE '%Pallet%' THEN 100
                ELSE 1 
            END 
        AS INTEGER) as quantity
    FROM demand_calc d
    -- JOINING POSTGRES TABLE DIRECTLY FROM TRINO!
    JOIN postgres.public.products p ON d.sku = p.sku
    WHERE d.net_demand > 0
    """

    cur.execute(sql_logic)
    print(f"✅ Success! Table {output_table} created in HDFS.")

    # Verification
    cur.execute(f"SELECT COUNT(*) FROM {output_table}")
    count = cur.fetchone()[0]
    print(f" Total Supplier Orders generated: {count}")

if __name__ == "__main__":
    main()


# How to Run It
# Install the Trino client in your orchestrator (if not already installed):

# docker exec orchestrator pip install trino
# (Or add trino to your requirements.txt)

# Run the script:

# docker exec orchestrator python scripts/run_pipeline_trino.py
# Update your Scheduler: Change run_job("run_pipeline.py") to run_job("run_pipeline_trino.py") in your orchestrator_scheduler.py file.
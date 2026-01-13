import os
import pandas as pd
import psycopg2

def pg_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "procurement_db"),
        user=os.getenv("POSTGRES_USER", "procurement_user"),
        password=os.getenv("POSTGRES_PASSWORD", "procurement_pass"),
    )

def read_sql_df(query: str) -> pd.DataFrame:
    conn = pg_connect()
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()
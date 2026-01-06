import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv #  pip install dotenv

load_dotenv()

def pg_connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

def read_sql_df(query: str) -> pd.DataFrame:
    conn = pg_connect()
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()
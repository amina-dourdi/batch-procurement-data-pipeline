import os
from trino.dbapi import connect

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", 8080))
TRINO_USER = os.getenv("TRINO_USER", "procurement")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "hive")

def ensure_schema(schema_name: str):
    """Create schema in Trino/Hive if it does not exist."""
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
    )
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    conn.close()
    print(f"[INFO] Schema '{schema_name}' ensured in Trino/Hive.")

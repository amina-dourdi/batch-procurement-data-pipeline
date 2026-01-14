import pandas as pd
from trino.dbapi import connect

conn = connect(
    host="trino",
    port=8080,
    user="admin",
    catalog='hive',    # the Hive catalog
    schema='default'
)
cur = conn.cursor()

# 2️⃣ Ensure schemas exist
schemas_to_create = ["hive.default", "hive.processed"]

for schema in schemas_to_create:
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    print(f"✅ Ensuring schema exists: {schema}")
    cur.execute(sql)

query = """
SELECT * FROM hive.default
"""


df = pd.read_sql(query, conn)
print(df)

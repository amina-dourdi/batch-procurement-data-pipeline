from pg_client import read_sql_df
df = read_sql_df('select distinct(package) from products')
print(df)
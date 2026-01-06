Great question 👍
Once the containers are **UP**, you should verify things **layer by layer**: HDFS → Trino → Postgres → End-to-end query.

Below is a **clean, practical checklist** you can follow.

---

# 1️⃣ Verify containers health

```bash
docker ps
```

You should see:

* namenode → **healthy**
* datanode1 / datanode2 → running
* postgres → **healthy**
* trino → **healthy**

If Trino is not healthy:

```bash
docker logs trino
```

---

# 2️⃣ Verify HDFS is working

### 2.1 Open HDFS Web UI

In your browser:

```
http://localhost:9870
```

✔ NameNode UI loads
✔ Datanodes visible (2)

---

### 2.2 Test HDFS from namenode container

```bash
docker exec -it namenode bash
```

Inside:

```bash
hdfs dfs -ls /
```

Create a directory:

```bash
hdfs dfs -mkdir /procurement
hdfs dfs -ls /
```

✔ If this works → HDFS is OK

(Optional test file)

```bash
echo "hello hdfs" > test.txt
hdfs dfs -put test.txt /procurement
hdfs dfs -ls /procurement
```

Exit:

```bash
exit
```

---

# 3️⃣ Verify Trino is running

### 3.1 Trino Web UI

Open:

```
http://localhost:8080
```

✔ You should see Trino UI
✔ Coordinator = running

---

### 3.2 Enter Trino CLI (best way)

```bash
docker exec -it trino trino
```

If CLI opens → Trino is alive.

---

# 4️⃣ Verify Trino catalogs

Inside Trino CLI:

```sql
SHOW CATALOGS;
```

Expected (example):

```text
hdfs
postgresql
system
```

If a catalog is missing → configuration issue.

---

# 5️⃣ Test Postgres through Trino

### 5.1 Show schemas

```sql
SHOW SCHEMAS FROM postgresql;
```

You should see:

```text
information_schema
public
```

---

### 5.2 Show tables

```sql
SHOW TABLES FROM postgresql.public;
```

If empty → schema exists but no tables (normal).

---

### 5.3 Create table via Trino (IMPORTANT TEST)

```sql
CREATE TABLE postgresql.public.test_table (
    id INTEGER,
    name VARCHAR
);
```

Insert data:

```sql
INSERT INTO postgresql.public.test_table VALUES
(1, 'apple'),
(2, 'banana');
```

Query:

```sql
SELECT * FROM postgresql.public.test_table;
```

✔ If this works → **Trino ↔ Postgres is OK**

---

## 6️⃣ Test HDFS via Trino (no Hive Metastore)

### Situation:

* When you ran:

```sql
SHOW SCHEMAS FROM hdfs;
```

* Trino returned:

```
information_schema
```

* **There was no `default` schema**.
* Then, when you tried:

```sql
SHOW TABLES FROM hdfs.default;
```

* Trino gave an error:

```
Schema 'default' does not exist
```

**Reason:**
You are using a **File-based Hive Metastore** (`hive.metastore=file`) and there is no pre-existing schema.
In this setup, **Trino does not automatically create a default schema** like a real Hive Metastore would. Any `CREATE TABLE` on a non-existent schema will fail.

---

### Solution:

1. **Create the schema manually:**

```sql
CREATE SCHEMA hdfs.default;
```

* `hdfs` is the **catalog** (the connection to HDFS in Trino)
* `default` is the **new schema** where you can create tables

2. **Verify the schema was created:**

```sql
SHOW SCHEMAS FROM hdfs;
```

* Now you should see:

```
information_schema
default
```

---

### After creating the schema:

* You can now create tables inside `hdfs.default`:

```sql
CREATE TABLE hdfs.default.test_orders (
  order_id INTEGER,
  product  VARCHAR,
  quantity INTEGER
)
WITH (
  external_location = 'hdfs://namenode:9000/user/hive/warehouse/test_orders',
  format = 'TEXTFILE'
);
```

* This creates an **external table** pointing to an HDFS folder.

---

### Important Notes:

* **Without a Hive Metastore**, Trino cannot create schemas or manage tables automatically.
* HDFS can still be used for **storing external files** like CSV or Parquet.
* Any schema or table must be **created manually** for Trino to recognize it.


---

# 7️⃣ End-to-end test (your real pipeline)

### Example flow:

1. Put data in HDFS
2. Query it via Trino
3. Insert results into Postgres

Example:

```bash
docker exec -it namenode bash
```

```bash
hdfs dfs -put /data/sample.csv /procurement/
```

Then in Trino (if CSV connector configured):

```sql
SELECT * FROM hdfs.procurement.sample;
```

---

# 8️⃣ Monitor stability (important for your previous errors)

### Check JVM pauses / memory

```bash
docker stats
```

If Trino memory is tight → increase:

```yaml
query.max-memory=2GB
memory.heap-headroom-per-node=1GB
```

---

# 9️⃣ What “working well” means ✅

You’re good if:

✔ HDFS UI loads
✔ Trino UI loads
✔ `SHOW CATALOGS` works
✔ Trino can query Postgres
✔ No repeating `Announcer delayed` errors
✔ No endless `Trino server still initializing`

---

## 🎯 Recommendation for your project

For a **real Big Data project**, next step should be:

🔹 Add **Hive Metastore**
🔹 Use **Parquet tables on HDFS**
🔹 Use Trino as **SQL engine**

If you want, I can:

* Add Hive Metastore to your compose
* Create real Trino tables on HDFS
* Design a **professor-ready validation demo**

Just tell me 👍

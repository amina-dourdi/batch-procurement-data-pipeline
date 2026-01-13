

# Orchestrator Service — Detailed Explanation

## Overview

The **orchestrator** service is the central execution component of the data platform.  
It acts as the **brain of the pipeline**, responsible for running:

- ETL scripts  
- Batch jobs  
- Data processing pipelines  
- Automation tasks  

It connects to:
- **HDFS** → raw and processed data storage  
- **PostgreSQL** → metadata and business rules  
- **Trino** → analytics and querying  

---

## Service Definition

```yaml
orchestrator:
  mem_limit: 256m
  cpus: 0.25
  build:
    context: .
    dockerfile: Dockerfile
  container_name: orchestrator
  hostname: orchestrator
  volumes:
    - ./scripts:/app/scripts
    - ./data:/app/data
    - ./output:/app/output
    - ./logs:/app/logs
    - ./sql:/app/sql
  environment:
    - HDFS_NAMENODE=hdfs://namenode:9000
    - PRESTO_HOST=trino
    - PRESTO_PORT=8080
    - POSTGRES_HOST=postgres
    - POSTGRES_PORT=5432
    - POSTGRES_DB=procurement_db
    - POSTGRES_USER=procurement_user
    - POSTGRES_PASSWORD=procurement_pass
    - TZ=Africa/Casablanca
  depends_on:
    trino:
      condition: service_healthy
    postgres:
      condition: service_healthy
  networks:
    - procurement-net
  command: tail -f /dev/null


---

## Line-by-Line Explanation

### 1. Service Name

```yaml
orchestrator:
```

Defines a Docker service named **orchestrator**.
Other containers can reach it via:

```
orchestrator
```

---

### 2. Resource Limits

```yaml
mem_limit: 256m
cpus: 0.25
```

Limits the resources used by this container.

* Prevents memory exhaustion
* Prevents CPU overuse
* Ideal for a service that mainly runs scripts and orchestration logic

---

### 3. Build Configuration

```yaml
build:
  context: .
  dockerfile: Dockerfile
```

Docker builds a **custom image** using the local `Dockerfile`.

This image typically contains:

* Python runtime
* Required libraries (pandas, psycopg2, hdfs client, etc.)

---

### 4. Identity

```yaml
container_name: orchestrator
hostname: orchestrator
```

* **container_name**: fixed name in Docker (`docker ps`)
* **hostname**: internal name used inside the network

---

### 5. Volumes (Shared Folders)

```yaml
volumes:
  - ./scripts:/app/scripts
  - ./data:/app/data
  - ./output:/app/output
  - ./logs:/app/logs
  - ./sql:/app/sql
```

These mappings connect local folders to the container.

| Host Folder | Container Path | Purpose                         |
| ----------- | -------------- | ------------------------------- |
| `./scripts` | `/app/scripts` | ETL and batch scripts           |
| `./data`    | `/app/data`    | Temporary files, HDFS downloads |
| `./output`  | `/app/output`  | Final exports                   |
| `./logs`    | `/app/logs`    | Logs                            |
| `./sql`     | `/app/sql`     | SQL queries                     |

### Why this matters

* You edit code locally
* It runs instantly in Docker
* No rebuild needed for script changes

---

### 6. Environment Variables

```yaml
environment:
  - HDFS_NAMENODE=hdfs://namenode:9000
```

Defines where HDFS is located.

```yaml
  - PRESTO_HOST=trino
  - PRESTO_PORT=8080
```

Defines where Trino is located.

```yaml
  - POSTGRES_HOST=postgres
  - POSTGRES_PORT=5432
  - POSTGRES_DB=procurement_db
  - POSTGRES_USER=procurement_user
  - POSTGRES_PASSWORD=procurement_pass
```

Defines PostgreSQL connection parameters for scripts.

Example usage in Python:

```python
import os, psycopg2

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
```

```yaml
  - TZ=Africa/Casablanca
```

Ensures correct timezone for:

* logs
* timestamps
* batch scheduling

---

### 7. Service Dependencies

```yaml
depends_on:
  trino:
    condition: service_healthy
  postgres:
    condition: service_healthy
```

The orchestrator will start **only after**:

* Trino is healthy
* PostgreSQL is healthy

This prevents:

* startup race conditions
* failed scripts due to unavailable services

---

### 8. Network Configuration

```yaml
networks:
  - procurement-net
```

Places the orchestrator in the same private Docker network as:

* namenode
* postgres
* trino

So it can connect using service names:

```
postgres:5432
trino:8080
namenode:9000
```

Docker DNS handles resolution automatically.

---

### 9. Container Command

```yaml
command: tail -f /dev/null
```

This command keeps the container **running forever**.

Without it:

* the container would start
* do nothing
* exit immediately

With it:

* the container stays alive
* you can run commands anytime:

```bash
docker exec -it orchestrator bash
```

Then run:

```bash
python /app/scripts/run_pipeline.py
```

So the orchestrator acts as a **permanent job runner environment**.

---

## Architectural Role

The orchestrator sits at the center of the data platform:

```
           +-----------+
           |  Trino    |
           +-----------+
                 |
                 |
+-----------+     |     +-----------+
|  HDFS     | <-- Orchestrator --> | PostgreSQL |
+-----------+                     +-----------+
```

It is responsible for:

* Orchestrating data flows
* Running ETL pipelines
* Automating batch jobs
* Acting as a future Airflow worker node

---

## Typical Workflow

1. Start the stack:

   ```bash
   docker-compose up -d
   ```

2. Enter the orchestrator:

   ```bash
   docker exec -it orchestrator bash
   ```

3. Run a pipeline:

   ```bash
   python /app/scripts/build_supplier_orders.py
   ```

4. The script:

   * reads from HDFS
   * reads from PostgreSQL
   * writes processed data back to HDFS

---

## Summary

> The **orchestrator** container is a dedicated execution environment that runs data pipelines and automation tasks, already connected to HDFS, PostgreSQL, and Trino, with controlled resources and persistent volumes — acting as the **brain of the data platform**.

```


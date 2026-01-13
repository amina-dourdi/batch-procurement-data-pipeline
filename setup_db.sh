#!/bin/bash

# --- WINDOWS GIT BASH FIX ---
export MSYS_NO_PATHCONV=1

# --- LOAD CONFIGURATION ---
if [ -f .env ]; then
    # Load environment variables automatically
    set -a
    source .env
    set +a
    echo "✅ Loaded configuration from .env"
else
    echo "❌ Error: .env file not found!"
    exit 1
fi

# --- SAFETY CHECK ---
# Check if critical variables are empty
if [ -z "$DB_USER" ] || [ -z "$CONTAINER_NAME" ]; then
    echo "❌ Error: Variables DB_USER or CONTAINER_NAME are missing in .env"
    echo "   Please check your .env file content."
    exit 1
fi

echo "=========================================="
echo " [Step 1] Generating fresh data with Python..."
echo "=========================================="
python scripts/generate_master_data.py

echo ""
echo "=========================================="
echo " [Step 2] Copying CSVs to Docker Container..."
echo "=========================================="

docker exec $CONTAINER_NAME mkdir -p //tmp/data_load

docker cp $CSV_DIR/suppliers.csv $CONTAINER_NAME://tmp/data_load/suppliers.csv
docker cp $CSV_DIR/market.csv $CONTAINER_NAME://tmp/data_load/market.csv
docker cp $CSV_DIR/products.csv $CONTAINER_NAME://tmp/data_load/products.csv

echo ""
echo "=========================================="
echo " [Step 3] Recreating Tables & Importing Data..."
echo "=========================================="

docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME <<EOF

-- A. DROP AND RECREATE TABLES
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS market CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;

CREATE TABLE suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50),
    contact_email VARCHAR(100),
    location VARCHAR(100)
);

CREATE TABLE market (
    market_id VARCHAR(20) PRIMARY KEY,
    location VARCHAR(100),
    type VARCHAR(50)
);

CREATE TABLE products (
    sku VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10, 2),
    supplier_id VARCHAR(20) REFERENCES suppliers(supplier_id),
    moq INT,
    mxoq INT,
    package VARCHAR(50),
    leadtime INT
);

-- B. COPY DATA FROM CSV
-- We use the COPY command to load the files we just moved to /tmp/data_load
COPY suppliers(supplier_id, name, country, contact_email, location) 
FROM '/tmp/data_load/suppliers.csv' DELIMITER ',' CSV HEADER;

COPY market(market_id, location, type) 
FROM '/tmp/data_load/market.csv' DELIMITER ',' CSV HEADER;

COPY products(sku, name, category, unit_price, supplier_id, moq, mxoq, package, leadtime) 
FROM '/tmp/data_load/products.csv' DELIMITER ',' CSV HEADER;

EOF

echo "✅ SUCCESS: Database '$DB_NAME' is ready and fully populated!"
-- sql/init_schema.sql

DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS market;
DROP TABLE IF EXISTS suppliers;

-- 1. Create Suppliers Table
CREATE TABLE suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    contact_email VARCHAR(100),
    location VARCHAR(100)
);

-- 2. Create Market Table (Points of Sale / Stores)
CREATE TABLE market (
    market_id VARCHAR(20) PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    type VARCHAR(50) -- e.g., 'Superstore', 'Express', 'Online'
);

-- 3. Create Products Table (with embedded Procurement Rules)
CREATE TABLE products (
    sku VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    unit_price DECIMAL(10, 2),
    supplier_id VARCHAR(20) REFERENCES suppliers(supplier_id), -- FK link
    moq INT DEFAULT 1,         -- Min Order Quantity
    mxoq INT,                  -- Max Order Quantity
    package VARCHAR(50),       -- Packaging type (e.g., 'Box of 12')
    leadtime INT               -- Days to deliver
);
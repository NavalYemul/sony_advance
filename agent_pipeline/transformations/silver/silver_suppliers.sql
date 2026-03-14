-- Silver Layer: Supplier dimension with SCD Type 2 history tracking
-- Tracks changes to supplier approval status and ingredient details

USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE silver_suppliers
COMMENT "SCD Type 2 supplier dimension tracking approval status and ingredient changes";

CREATE FLOW silver_suppliers_cdc AS AUTO CDC INTO silver_suppliers
FROM STREAM(sony_dev.bronze.bronze_suppliers)
KEYS (supplierID)
SEQUENCE BY _ingestion_timestamp
COLUMNS * EXCEPT (_source_table)
STORED AS SCD TYPE 2
TRACK HISTORY ON name, ingredient, continent, city, district, size, approved;

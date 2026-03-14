-- Silver Layer: Franchise dimension with SCD Type 2 history tracking
-- Tracks changes to franchise location and operational details

USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE silver_franchises
COMMENT "SCD Type 2 franchise dimension tracking location, size, and supplier changes";

CREATE FLOW silver_franchises_cdc AS AUTO CDC INTO silver_franchises
FROM STREAM(sony_dev.bronze.bronze_franchises)
KEYS (franchiseID)
SEQUENCE BY _ingestion_timestamp
COLUMNS * EXCEPT (_source_table)
STORED AS SCD TYPE 2
TRACK HISTORY ON name, city, district, zipcode, country, size, longitude, latitude, supplierID;

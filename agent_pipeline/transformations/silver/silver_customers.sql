-- Silver Layer: Customer dimension with SCD Type 2 history tracking
-- Tracks changes to customer contact details and demographics

USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT "SCD Type 2 customer dimension tracking address, contact, and demographic changes";

CREATE FLOW silver_customers_cdc AS AUTO CDC INTO silver_customers
FROM STREAM(sony_dev.bronze.bronze_customers)
KEYS (customerID)
SEQUENCE BY _ingestion_timestamp
COLUMNS * EXCEPT (_source_table)
STORED AS SCD TYPE 2
TRACK HISTORY ON first_name, last_name, email_address, phone_number, address, city, state, country, postal_zip_code, gender;

-- Bronze Layer: Raw customer ingestion with audit metadata
-- Source: samples.bakehouse.sales_customers

CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT "Raw customer data ingested from bakehouse with audit metadata"
AS SELECT
  *,
  current_timestamp() AS _ingestion_timestamp,
  'samples.bakehouse.sales_customers' AS _source_table
FROM STREAM(samples.bakehouse.sales_customers);

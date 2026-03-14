-- Bronze Layer: Raw transaction ingestion with audit metadata
-- Source: samples.bakehouse.sales_transactions

CREATE OR REFRESH STREAMING TABLE bronze_transactions
COMMENT "Raw sales transactions ingested from bakehouse with audit metadata"
AS SELECT
  *,
  current_timestamp() AS _ingestion_timestamp,
  'samples.bakehouse.sales_transactions' AS _source_table
FROM STREAM(samples.bakehouse.sales_transactions);

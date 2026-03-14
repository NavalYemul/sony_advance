-- Bronze Layer: Raw supplier ingestion with audit metadata
-- Source: samples.bakehouse.sales_suppliers

CREATE OR REFRESH STREAMING TABLE bronze_suppliers
COMMENT "Raw supplier data ingested from bakehouse with audit metadata"
AS SELECT
  *,
  current_timestamp() AS _ingestion_timestamp,
  'samples.bakehouse.sales_suppliers' AS _source_table
FROM STREAM(samples.bakehouse.sales_suppliers);

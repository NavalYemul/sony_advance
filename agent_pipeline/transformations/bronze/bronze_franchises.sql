-- Bronze Layer: Raw franchise ingestion with audit metadata
-- Source: samples.bakehouse.sales_franchises

CREATE OR REFRESH STREAMING TABLE bronze_franchises
COMMENT "Raw franchise data ingested from bakehouse with audit metadata"
AS SELECT
  *,
  current_timestamp() AS _ingestion_timestamp,
  'samples.bakehouse.sales_franchises' AS _source_table
FROM STREAM(samples.bakehouse.sales_franchises);

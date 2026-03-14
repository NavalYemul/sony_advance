-- Bronze Layer: Raw customer review ingestion with audit metadata
-- Source: samples.bakehouse.media_customer_reviews

CREATE OR REFRESH STREAMING TABLE bronze_customer_reviews
COMMENT "Raw customer reviews ingested from bakehouse with audit metadata"
AS SELECT
  *,
  current_timestamp() AS _ingestion_timestamp,
  'samples.bakehouse.media_customer_reviews' AS _source_table
FROM STREAM(samples.bakehouse.media_customer_reviews);

-- Silver Layer: Cleaned and validated customer reviews
-- Data quality checks ensure review integrity and valid references

USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE silver_customer_reviews (
  CONSTRAINT valid_review_id EXPECT (review_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_review_content EXPECT (review IS NOT NULL AND LENGTH(TRIM(review)) > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_franchise_ref EXPECT (franchiseID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_review_date EXPECT (review_date IS NOT NULL)
)
COMMENT "Cleaned customer reviews with data quality validation"
AS SELECT
  new_id AS review_id,
  review,
  franchiseID,
  review_date,
  _ingestion_timestamp
FROM STREAM(sony_dev.bronze.bronze_customer_reviews);

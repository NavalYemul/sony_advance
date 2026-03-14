-- Silver Layer: Cleaned and validated transactions with data quality enforcement
-- PII (cardNumber) excluded; columns standardized to snake_case

USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE silver_transactions (
  CONSTRAINT valid_transaction_id EXPECT (transactionID IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_customer_id EXPECT (customerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_franchise_id EXPECT (franchiseID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT positive_unit_price EXPECT (unit_price > 0),
  CONSTRAINT positive_total_price EXPECT (total_price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payment_method EXPECT (payment_method IN ('visa', 'mastercard', 'amex', 'discover'))
)
COMMENT "Cleaned sales transactions with data quality enforcement and PII removed"
AS SELECT
  transactionID,
  customerID,
  franchiseID,
  dateTime AS transaction_datetime,
  product,
  quantity,
  unitPrice AS unit_price,
  totalPrice AS total_price,
  paymentMethod AS payment_method,
  _ingestion_timestamp
FROM STREAM(sony_dev.bronze.bronze_transactions);

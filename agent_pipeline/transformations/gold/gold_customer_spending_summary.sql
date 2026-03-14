-- Gold Layer: Customer spending analysis
-- Aggregates per-customer spending patterns enriched with customer demographics

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_spending_summary
COMMENT "Customer lifetime value, spending frequency, and demographic enrichment"
AS SELECT
  t.customerID,
  c.first_name,
  c.last_name,
  c.email_address,
  c.city AS customer_city,
  c.country AS customer_country,
  c.gender,
  COUNT(*) AS total_orders,
  SUM(t.quantity) AS total_units_purchased,
  SUM(t.total_price) AS lifetime_value,
  ROUND(AVG(t.total_price), 2) AS avg_order_value,
  MIN(t.transaction_datetime) AS first_purchase_date,
  MAX(t.transaction_datetime) AS last_purchase_date,
  COUNT(DISTINCT t.product) AS unique_products_purchased,
  COUNT(DISTINCT t.franchiseID) AS unique_franchises_visited
FROM sony_dev.silver.silver_transactions t
LEFT JOIN sony_dev.silver.silver_customers c
  ON t.customerID = c.customerID
  AND c.__END_AT IS NULL
GROUP BY ALL;

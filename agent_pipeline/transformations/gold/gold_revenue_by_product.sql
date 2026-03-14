-- Gold Layer: Product performance analysis
-- Aggregates sales data by product for ranking and trend insights

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_by_product
COMMENT "Product-level sales performance ranking and metrics"
AS SELECT
  product,
  COUNT(*) AS total_orders,
  SUM(quantity) AS total_units_sold,
  SUM(total_price) AS total_revenue,
  ROUND(AVG(unit_price), 2) AS avg_unit_price,
  ROUND(AVG(total_price), 2) AS avg_order_value,
  COUNT(DISTINCT customerID) AS unique_customers,
  COUNT(DISTINCT franchiseID) AS selling_franchises,
  MIN(transaction_datetime) AS first_sale_date,
  MAX(transaction_datetime) AS last_sale_date
FROM sony_dev.silver.silver_transactions
GROUP BY ALL;

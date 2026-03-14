-- Gold Layer: Revenue analysis by franchise
-- Aggregates transaction data enriched with franchise dimension details

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_by_franchise
COMMENT "Revenue metrics per franchise including location and operational details"
AS SELECT
  t.franchiseID,
  f.name AS franchise_name,
  f.city AS franchise_city,
  f.country AS franchise_country,
  f.size AS franchise_size,
  COUNT(*) AS total_orders,
  SUM(t.quantity) AS total_units_sold,
  SUM(t.total_price) AS total_revenue,
  ROUND(AVG(t.total_price), 2) AS avg_order_value,
  MIN(t.transaction_datetime) AS first_order_date,
  MAX(t.transaction_datetime) AS last_order_date
FROM sony_dev.silver.silver_transactions t
LEFT JOIN sony_dev.silver.silver_franchises f
  ON t.franchiseID = f.franchiseID
  AND f.__END_AT IS NULL
GROUP BY ALL;

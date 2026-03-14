-- Gold Layer: Franchise performance scorecard
-- Combines franchise dimension, supplier info, revenue metrics, and review data

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW gold_franchise_performance
COMMENT "Franchise performance scorecard combining revenue, supply chain, and review metrics"
AS SELECT
  f.franchiseID,
  f.name AS franchise_name,
  f.city,
  f.country,
  f.size AS franchise_size,
  s.name AS supplier_name,
  s.ingredient AS supplier_ingredient,
  -- Revenue metrics
  COALESCE(rev.total_orders, 0) AS total_orders,
  COALESCE(rev.total_revenue, 0) AS total_revenue,
  COALESCE(rev.total_units_sold, 0) AS total_units_sold,
  COALESCE(rev.avg_order_value, 0) AS avg_order_value,
  COALESCE(rev.unique_products, 0) AS unique_products,
  COALESCE(rev.unique_customers, 0) AS unique_customers,
  -- Review metrics
  COALESCE(rvw.total_reviews, 0) AS total_reviews,
  rvw.earliest_review,
  rvw.latest_review
FROM sony_dev.silver.silver_franchises f
LEFT JOIN sony_dev.silver.silver_suppliers s
  ON f.supplierID = s.supplierID
  AND s.__END_AT IS NULL
LEFT JOIN (
  SELECT
    franchiseID,
    COUNT(*) AS total_orders,
    SUM(total_price) AS total_revenue,
    SUM(quantity) AS total_units_sold,
    ROUND(AVG(total_price), 2) AS avg_order_value,
    COUNT(DISTINCT product) AS unique_products,
    COUNT(DISTINCT customerID) AS unique_customers
  FROM sony_dev.silver.silver_transactions
  GROUP BY franchiseID
) rev ON f.franchiseID = rev.franchiseID
LEFT JOIN (
  SELECT
    franchiseID,
    COUNT(*) AS total_reviews,
    MIN(review_date) AS earliest_review,
    MAX(review_date) AS latest_review
  FROM sony_dev.silver.silver_customer_reviews
  GROUP BY franchiseID
) rvw ON f.franchiseID = rvw.franchiseID
WHERE f.__END_AT IS NULL;

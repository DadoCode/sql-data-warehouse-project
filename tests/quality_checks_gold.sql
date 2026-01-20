/*
===============================================================================
Quality Checks - Gold Layer
===============================================================================
Script Purpose:
    This script performs quality checks on the 'gold' schema dimensional model
    to ensure data integrity, referential integrity, and proper star schema
    implementation. It validates:
    - Uniqueness of surrogate keys in dimension tables
    - Referential integrity between fact and dimension tables
    - Data model connectivity and join success rates
    - NULL values in critical fields

Usage Notes:
    - Run these checks after creating gold layer views
    - Investigate and resolve any discrepancies found during the checks
===============================================================================
*/

-- ============================================================================
-- Checking 'gold.dim_customers'
-- ============================================================================

-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Check for NULL Customer Keys
-- Expectation: No results
SELECT *
FROM gold.dim_customers
WHERE customer_key IS NULL OR customer_id IS NULL;


-- ============================================================================
-- Checking 'gold.dim_product'
-- ============================================================================

-- Check for Uniqueness of Product Key in gold.dim_product
-- Expectation: No results
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_product
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Check for NULL Product Keys
-- Expectation: No results
SELECT *
FROM gold.dim_product
WHERE product_key IS NULL OR product_id IS NULL;


-- ============================================================================
-- Checking 'gold.fact_sales'
-- ============================================================================

-- Check the data model connectivity between fact and dimensions
-- Expectation: All rows should have matching dimension keys (no NULLs)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_product p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;

-- Check for orphaned records in fact_sales (missing customer dimension)
-- Expectation: No results
SELECT COUNT(*) AS orphaned_customer_records
FROM gold.fact_sales f
WHERE f.customer_key IS NULL;

-- Check for orphaned records in fact_sales (missing product dimension)
-- Expectation: No results
SELECT COUNT(*) AS orphaned_product_records
FROM gold.fact_sales f
WHERE f.product_key IS NULL;

-- Check for NULL values in critical fact measures
-- Expectation: No results (or minimal results depending on business rules)
SELECT *
FROM gold.fact_sales
WHERE sales_amount IS NULL 
   OR quantity IS NULL 
   OR price IS NULL;

-- Validate fact table row count matches source
-- Expectation: Counts should match
SELECT 
    'silver.crm_sales_details' AS source_table,
    COUNT(*) AS row_count
FROM silver.crm_sales_details
UNION ALL
SELECT 
    'gold.fact_sales' AS gold_table,
    COUNT(*) AS row_count
FROM gold.fact_sales;

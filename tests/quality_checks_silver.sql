/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schema. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-- ============================================================================
-- Checking 'silver.crm_cust_info'
-- ============================================================================

-- Check For Nulls or Duplicates in Primary Key (Bronze)
-- Expectation: No Results

SELECT
    cst_id,
    count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces and trimming (Bronze)
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency (Bronze)
-- Expectation: All values conform to the standard
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


-- Check For Nulls or Duplicates in Primary Key (Silver)
-- Expectation: No Results
SELECT
    cst_id,
    count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces (Silver)
-- Expectation: No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency (Silver)
-- Expectation: All values conform to the standard
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


-- ============================================================================
-- Checking 'silver.crm_prd_info'
-- ============================================================================

-- Check For Nulls or Duplicates in Primary Key (Bronze)
-- Expectation: No Results
SELECT
    prd_id,
    count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces and trimming (Bronze)
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or Negative Numbers (Bronze)
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost is NULL;

-- Data Standardization & Consistency (Bronze)
-- Expectation: All values conform to the standard
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Start and End Date (Bronze)
-- Take it into Excel to see it clearly first
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


-- Check For Nulls or Duplicates in Primary Key (Silver)
-- Expectation: No Results
SELECT
    prd_id,
    count(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL;  

-- Check for unwanted spaces and trimming (Silver)
-- Expectation: No Results
SELECT prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or Negative Numbers (Silver)
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost is NULL;

-- Data Standardization & Consistency (Silver)
-- Expectation: All values conform to the standard
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Start and End Date (Silver)
-- Take it into Excel to see it clearly first
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ============================================================================
-- Checking 'silver.crm_sales_details'
-- ============================================================================

-- Check for Invalid Dates (Bronze)
SELECT
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE 
    sls_due_dt <= 0 
    or LEN(sls_due_dt) != 8 
    or sls_due_dt > 20500101 
    or sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Bronze)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt;

-- Check Data Consistency: Between Sales, Quantity, and Price (Bronze)
-- Sales = Quantity * Price
-- Values must not be Null, Zero, or Negative
SELECT
    sls_quantity,
    sls_price,
    sls_sales
FROM bronze.crm_sales_details
WHERE 
    sls_sales != sls_quantity * sls_price 
    OR sls_sales IS NULL 
    OR sls_sales <= 0 
    OR sls_quantity IS NULL 
    OR sls_quantity <= 0 
    OR sls_price IS NULL 
    OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price;


-- Check for Invalid Dates in Silver Table
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt;

-- Check Data Consistency in Silver Table: Between Sales, Quantity, and Price
SELECT DISTINCT
    sls_quantity,
    sls_price,
    sls_sales
FROM silver.crm_sales_details
WHERE 
    sls_sales != sls_quantity * sls_price 
    OR sls_sales IS NULL OR sls_sales <= 0 
    OR sls_quantity IS NULL 
    OR sls_quantity <= 0 
    OR sls_price IS NULL 
    OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price;


-- ============================================================================
-- Checking 'silver.erp_cust_az12'
-- ============================================================================

-- Check if cid is corrected and extra characters are removed (Bronze)
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
         ELSE cid
    END as cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
         ELSE cid
    END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Identify out-of-range dates (Bronze)
SELECT DISTINCT
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Check Gen for Data Standardization and Consistency (Bronze)
SELECT DISTINCT 
    gen
FROM bronze.erp_cust_az12;


-- Check out-of-range dates (Silver)
SELECT DISTINCT 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Check Gen for Data Standardization and Consistency (Silver)
SELECT DISTINCT 
    gen
FROM silver.erp_cust_az12;


-- ============================================================================
-- Checking 'silver.erp_loc_a101'
-- ============================================================================

-- Data Standardization (Bronze)
SELECT DISTINCT 
    cntry 
FROM bronze.erp_loc_a101;

SELECT DISTINCT
    cntry as oldcntry,
    CASE
        WHEN UPPER(cntry) LIKE 'DE%' OR UPPER(cntry) LIKE 'GERMANY%' THEN 'Germany'
        WHEN UPPER(cntry) LIKE 'US%' OR UPPER(cntry) LIKE 'USA%' OR UPPER(cntry) LIKE 'UNITED STATES%' THEN 'United States'
        WHEN UPPER(cntry) LIKE 'UNITED KINGDOM%' OR UPPER(cntry) LIKE 'UK%' THEN 'United Kingdom'
        WHEN UPPER(cntry) LIKE 'FRANCE%' OR UPPER(cntry) LIKE 'FR%' THEN 'France'
        WHEN UPPER(cntry) LIKE 'AUSTRALIA%' OR UPPER(cntry) LIKE 'AU%' THEN 'Australia'
        WHEN UPPER(cntry) LIKE 'CANADA%' OR UPPER(cntry) LIKE 'CA%' THEN 'Canada'
        ELSE 'N/A'
    END AS cntry
FROM bronze.erp_loc_a101;

-- Data Standardization (Silver)
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101;


-- ============================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ============================================================================

-- Check Unwanted Spaces (Bronze)
SELECT 
    *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Check data standardization (Bronze)
SELECT DISTINCT
    maintenance,
    CASE WHEN TRIM(maintenance) = 'Yes ' THEN 'Yes'
         ELSE maintenance
    END AS maintenance
FROM bronze.erp_px_cat_g1v2;

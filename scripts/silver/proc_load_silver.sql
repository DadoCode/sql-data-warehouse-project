/*
===============================================================================
Silver Layer Data Load Procedure
===============================================================================
Procedure Name: silver.load_silver

Description:
    This procedure transforms and loads data from bronze layer tables into the
    silver layer. It performs data cleaning, standardization, enrichment, and
    quality improvements to prepare data for analytical use in the gold layer.

Transformations Applied:
    - Data cleansing (trimming whitespace, removing invalid characters)
    - Standardization (normalizing codes to readable values)
    - Data quality fixes (handling nulls, invalid dates, incorrect calculations)
    - Deduplication (removing duplicate records)
    - Data enrichment (deriving new columns, calculating missing values)
    - Format conversion (date standardization, data type casting)

Tables Loaded:
    CRM Tables:
        - silver.crm_cust_info: Deduplicated customer records with normalized values
        - silver.crm_prd_info: Product data with extracted categories and calculated end dates
        - silver.crm_sales_details: Sales transactions with validated dates and recalculated metrics
    
    ERP Tables:
        - silver.erp_cust_az12: Customer demographics with standardized gender and validated dates
        - silver.erp_loc_a101: Location data with normalized country names
        - silver.erp_px_cat_g1v2: Product categories with cleaned maintenance flags

Features:
    - Individual table load duration tracking
    - Total batch execution time monitoring
    - Comprehensive error handling with detailed error messages
    - Truncate and load pattern for full refresh
    - Inline comments explaining each transformation

Input Parameters:
    None - Procedure runs without requiring input arguments

Return Values:
    None - Results are reflected in the loaded silver tables

Example Execution:
    EXEC silver.load_silver;

Notes:
    - Bronze layer tables must be loaded before running this procedure
    - All transformations use defensive coding to handle edge cases
    - Default values ('N/A', NULL, 0) are applied where data is missing or invalid

===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '==============================';
        PRINT 'Loading silver layer';
        PRINT '==============================';

        PRINT '-------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------';

        /* ===============================
           silver.crm_cust_info
        =============================== */
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_martial_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname_trimmed, -- Trim whitespace
            TRIM(cst_lastname) AS cst_lastname_trimmed,   -- Trim whitespace
            CASE 
                WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
                ELSE 'N/A' -- Normalize marital status and handle missing data
            END AS cst_martial_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'N/A' -- Normalize gender and handle missing data
            END AS cst_gndr, 
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id 
                    ORDER BY cst_create_date DESC
                ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1; -- Select the most recent record per customer and remove duplicates

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
              + ' seconds';
        PRINT '-------------------------------';


        /* ===============================
           silver.crm_prd_info
        =============================== */
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Derived column by extracting category
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Derived column by extracting product key
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost, -- Handle NULLs by replacing with 0
            CASE UPPER(TRIM(prd_line))       -- Standardize and normalize product line
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A' -- Handle unexpected values
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Remove time component
            CAST(
                LEAD(prd_start_dt) 
                OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
                AS DATE
            ) AS prd_end_dt -- Data enrichment: calculate end date from next start date
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
              + ' seconds';
        PRINT '-------------------------------';


        /* ===============================
           silver.crm_sales_details
        =============================== */
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
                THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt, -- Handle invalid order dates

            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
                THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt, -- Handle invalid ship dates

            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
                THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt, -- Handle invalid due dates

            CASE 
                WHEN sls_sales IS NULL 
                     OR sls_sales <= 0 
                     OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales, -- Recalculate sales if missing or incorrect

            sls_quantity,

            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price -- Derive price if missing or invalid

        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
              + ' seconds';
        PRINT '-------------------------------';


        PRINT '-------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------';

        /* ===============================
           silver.erp_cust_az12
        =============================== */
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
                ELSE cid
            END AS cid, -- Remove 'NAS' prefix if present

            CASE 
                WHEN bdate > GETDATE() THEN NULL
                WHEN bdate < '1924-01-01' THEN NULL
                ELSE bdate
            END AS bdate, -- Handle future and out-of-range dates

            CASE 
                WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
                WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male'
                ELSE 'N/A'
            END AS gen -- Standardize gender values

        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
              + ' seconds';
        PRINT '-------------------------------';


        /* ===============================
           silver.erp_loc_a101
        =============================== */
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', '') AS cid, -- Remove unwanted characters
            CASE
                WHEN UPPER(cntry) LIKE 'DE%' THEN 'Germany'
                WHEN UPPER(cntry) LIKE 'US%' THEN 'United States'
                WHEN UPPER(cntry) LIKE 'UNITED KINGDOM%' OR UPPER(cntry) LIKE 'UK%' THEN 'United Kingdom'
                WHEN UPPER(cntry) LIKE 'FR%' THEN 'France'
                WHEN UPPER(cntry) LIKE 'AU%' THEN 'Australia'
                WHEN UPPER(cntry) LIKE 'CA%' THEN 'Canada'
                ELSE 'N/A'
            END AS cntry -- Normalize country values
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
              + ' seconds';
        PRINT '-------------------------------';


        /* ===============================
           silver.erp_px_cat_g1v2
        =============================== */
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT 
            id,
            cat,
            subcat,
            CASE
                WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(
                        maintenance, CHAR(160), ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) = 'YES'
                THEN 'Yes'
                WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(
                        maintenance, CHAR(160), ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) = 'NO'
                THEN 'No'
                ELSE 'N/A'
            END AS maintenance -- Normalize Yes / No values
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
              + ' seconds';
        PRINT '-------------------------------';


        SET @batch_end_time = GETDATE();
        PRINT '==============================';
        PRINT 'Loading silver layer completed successfully';
        PRINT 'Total Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) 
              + ' seconds';
        PRINT '==============================';

    END TRY
    BEGIN CATCH
        PRINT '==============================';
        PRINT 'Error occurred while loading silver layer';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
        PRINT '==============================';
    END CATCH
END;

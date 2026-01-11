/*
===============================================================================
Bronze Layer Data Load Procedure
===============================================================================
Procedure Name: bronze.load_bronze

Description:
    This procedure ingests raw data from CSV source files into bronze layer tables.
    The bronze layer serves as the initial landing zone for unprocessed data from
    both CRM and ERP systems.

Operations Performed:
    - Clears existing data from all bronze tables (TRUNCATE operation)
    - Loads fresh data from CSV files using BULK INSERT statements
    - Tracks load duration for each table individually
    - Captures total execution time for the entire batch process

Input Parameters:
    None - This procedure runs without requiring any input arguments

Return Values:
    None - Results are reflected in the loaded bronze tables

Example Execution:
    EXEC bronze.load_bronze;

Notes:
    - CSV files must exist in /tmp/ directory within the SQL Server container
    - All source files are expected to have headers (FIRSTROW = 2)
    - Comma-delimited format is assumed for all CSV files
    - TABLOCK hint is used for performance optimization during bulk loads

===============================================================================
*/


ALTER   PROCEDURE bronze.load_bronze AS
BEGIN

  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
  BEGIN TRY
      SET @batch_start_time = GETDATE();
      PRINT '==============================';
      PRINT 'Loading bronze layer';
      PRINT '==============================';
      
      PRINT '-------------------------------';
      PRINT 'Loading CRM Tables';
      PRINT '-------------------------------';

      SET @start_time = GETDATE();
      PRINT '>>> Truncating Table: bronze.crm_cust_info';
      TRUNCATE TABLE bronze.crm_cust_info;

      PRINT '>>> Inserting Data into Table: bronze.crm_cust_info';
      BULK INSERT bronze.crm_cust_info
      FROM '/tmp/cust_info.csv'
      WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
      );
      SET @end_time = GETDATE();
      PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '-------------------------------';


      SET @start_time = GETDATE();
      PRINT '>>> Truncating Table: bronze.crm_prd_info';
      TRUNCATE TABLE bronze.crm_prd_info;

      PRINT '>>> Inserting Data into Table: bronze.crm_prd_info';
      BULK INSERT bronze.crm_prd_info
      FROM '/tmp/prd_info.csv'
      WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
      );
      SET @end_time = GETDATE();
      PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '-------------------------------';

      SET @start_time = GETDATE();
      PRINT '>>> Truncating Table: bronze.crm_sales_details';
      TRUNCATE TABLE bronze.crm_sales_details;

      PRINT '>>> Inserting Data into Table: bronze.crm_sales_details';
      BULK INSERT bronze.crm_sales_details
      FROM '/tmp/sales_details.csv'
      WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
      );
      SET @end_time = GETDATE();
      PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '-------------------------------';


      PRINT '-------------------------------';
      PRINT 'Loading ERP Tables';
      PRINT '-------------------------------';

      SET @start_time = GETDATE();      
      PRINT '>>> Truncating Table: bronze.erp_CUST_AZ12';
      TRUNCATE TABLE bronze.erp_CUST_AZ12;

      PRINT '>>> Inserting Data into Table: bronze.erp_CUST_AZ12';
      BULK INSERT bronze.erp_cust_az12
      FROM '/tmp/CUST_AZ12.csv'
      WITH (
          FIRSTROW = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
      );
      SET @end_time = GETDATE();
      PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '-------------------------------';


      SET @start_time = GETDATE();
      PRINT '>>> Truncating Table: bronze.erp_LOC_A101';
      TRUNCATE TABLE bronze.erp_LOC_A101;

      PRINT '>>> Inserting Data into Table: bronze.erp_LOC_A101';
      BULK INSERT bronze.erp_LOC_A101
      FROM '/tmp/LOC_A101.csv'
      WITH (
          FIRSTROW = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
      );
      SET @end_time = GETDATE();
      PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '-------------------------------';

      
      SET @start_time = GETDATE();
      PRINT '>>> Truncating Table: bronze.erp_px_cat_g1v2';
      TRUNCATE TABLE bronze.erp_px_cat_g1v2;

      PRINT '>>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
      BULK INSERT bronze.erp_px_cat_g1v2
      FROM '/tmp/px_cat_g1v2.csv'
      WITH (
          FIRSTROW = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
      );
      SET @end_time = GETDATE();
      PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '-------------------------------';

      SET @batch_end_time = GETDATE();
      PRINT 'Total Bronze Layer Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '==============================';
      PRINT 'Loading bronze layer completed successfully';
      PRINT '-Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds';
      PRINT '==============================';
  END TRY
  BEGIN CATCH
    PRINT '==============================';
    PRINT 'Error occurred while loading bronze layer: ' + ERROR_MESSAGE();
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
    PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
    PRINT '==============================';
  END CATCH
END

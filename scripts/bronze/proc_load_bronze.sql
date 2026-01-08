/*
======================================================================
Stored Procedure: bronze.load_bronze
Script Description: 
    This stored procedure loads data into the Bronze layer tables
    It first truncates the target tables and then performs bulk insert.

Parameters: None

Usage:
    EXEC bronze.load_bronze;
======================================================================
*/

USE DataWarehouse
GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\MERCY\Desktop\data_warehouse_project\datasets\source_crm\cust_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>> ---------------';

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.crm_prod_info';
		TRUNCATE TABLE bronze.crm_prod_info;

		PRINT '>> Inserting Data Into: bronze.crm_prod_info';
		BULK INSERT bronze.crm_prod_info
		FROM 'C:\Users\MERCY\Desktop\data_warehouse_project\datasets\source_crm\prd_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>> ---------------';


		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\MERCY\Desktop\data_warehouse_project\datasets\source_crm\sales_details.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>> ---------------';


		PRINT '--------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------------';
		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_cust';
		TRUNCATE TABLE bronze.erp_cust;

		PRINT '>> Inserting Data Into: bronze.erp_cust';
		BULK INSERT bronze.erp_cust
		FROM 'C:\Users\MERCY\Desktop\data_warehouse_project\datasets\source_erp\cust.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>> ---------------';

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_loc';
		TRUNCATE TABLE bronze.erp_loc;

		PRINT '>> Inserting Data Into: bronze.erp_loc';
		BULK INSERT bronze.erp_loc
		FROM 'C:\Users\MERCY\Desktop\data_warehouse_project\datasets\source_erp\loc.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>> ---------------';

		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_cat';
		TRUNCATE TABLE bronze.erp_cat;

		PRINT '>> Inserting Data Into: bronze.erp_cat';
		BULK INSERT bronze.erp_cat
		FROM 'C:\Users\MERCY\Desktop\data_warehouse_project\datasets\source_erp\cat.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>> ---------------';

		SET @batch_end_time = GETDATE();
		PRINT '==============================================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + 'seconds';
		PRINT '==============================================================';
		
	END TRY
	BEGIN CATCH
		PRINT '===============================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS VARCHAR);
	END CATCH
END
GO
EXEC bronze.load_bronze
 
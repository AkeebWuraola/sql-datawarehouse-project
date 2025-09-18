/*
=====================================================================================
Sript Purpose:
This stored procedure loads data into the bronze schema from external CSV files. 
It truncates the bronze tables before loading the data
It Uses the 'bulk insert' to load data from csv files to the tables
=====================================================================================
*/
create or alter procedure bronze.stp_load_bronze as 

begin
	declare @start_time datetime, @end_time datetime;
	declare @prd_start_time datetime, @prd_end_time datetime;
	

	begin try
		set @prd_start_time = getdate();

		print '======================================================================';
		print 'Loading Bronze Layer';
		print '======================================================================';

		print '----------------------------------------------------------------------';
		print 'Loading CRM Tables';
		print '----------------------------------------------------------------------';

		---write sql queries to extract and load files into the dw
		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_cust_info ';
		truncate table bronze.crm_cust_info
	
		print '>> Inserting Data Into: bronze.crm_cust_info ';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\MINE\PORTFOLIO\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2, --specifies the beginning of your data
			fieldterminator = ',', --specifies the delimeter
			tablock --locks the table while insert is ongoing
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'

		print '>> ------------------------------------'


		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_prd_info ';
		truncate table bronze.crm_prd_info

		print '>> Inserting Data Into: bronze.crm_prd_info ';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\MINE\PORTFOLIO\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2, --specifies the beginning of your data
			fieldterminator = ',', --specifies the delimeter
			tablock --locks the table while insert is ongoing
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'

		print '>> ------------------------------------'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_sales_details ';
		truncate table bronze.crm_sales_details

		print '>> Inserting Data Into: bronze.crm_sales_details ';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\MINE\PORTFOLIO\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2, --specifies the beginning of your data
			fieldterminator = ',', --specifies the delimeter
			tablock --locks the table while insert is ongoing
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'

		print '>> ------------------------------------'


		print '----------------------------------------------------------------------';
		print 'Loading ERP Tables';
		print '----------------------------------------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_cust_az12 ';
		truncate table bronze.erp_cust_az12

		print '>> Inserting Data Into: bronze.erp_cust_az12 ';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\MINE\PORTFOLIO\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2, --specifies the beginning of your data
			fieldterminator = ',', --specifies the delimeter
			tablock --locks the table while insert is ongoing
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'

		print '>> ------------------------------------'


		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_loc_a101 ';
		truncate table bronze.erp_loc_a101

		print '>> Inserting Data Into: bronze.erp_loc_a101 ';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\MINE\PORTFOLIO\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2, --specifies the beginning of your data
			fieldterminator = ',', --specifies the delimeter
			tablock --locks the table while insert is ongoing
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'

		print '>> ------------------------------------'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2 ';
		truncate table bronze.erp_px_cat_g1v2

		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2 ';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\MINE\PORTFOLIO\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2, --specifies the beginning of your data
			fieldterminator = ',', --specifies the delimeter
			tablock --locks the table while insert is ongoing
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'

		print '>> ------------------------------------'


		set @prd_end_time = getdate();

		print '>> ======================================================================'
		print '>> Load Duration of Bronze Layer: ' + cast(datediff(second,@prd_start_time,@prd_end_time) as nvarchar) +' seconds'
		print '>> ======================================================================'

	end try
	begin catch
		---create a log table to add the errors
		print '======================================================================';
		print 'Error Occured during Loading bronze layer';
		print 'Error Message ' + error_message();
		print 'Error Message ' + cast(error_number() as nvarchar);
		print 'Error Message ' + cast(error_state() as nvarchar);
		print '======================================================================';
	end catch
end

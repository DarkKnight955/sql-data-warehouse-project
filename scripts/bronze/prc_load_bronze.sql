/* 
---------------------------------------------------
##This script represents the procedure of loading data from sources to bronze layer
---------------------------------------------------
*/


USE [DataWarehouse]
GO
/****** Object:  StoredProcedure [bronze].[load_bronze]    Script Date: 7/9/2025 1:23:04 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[load_bronze]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [bronze].[load_bronze] AS' 
END
GO
ALTER   PROCEDURE [bronze].[load_bronze] as

begin

declare @batch_start_time datetime,@batch_end_time datetime, @batch_duration varchar;

set @batch_start_time = getdate();
	
	begin try

		declare @start_time datetime,@end_time datetime, @duration varchar;

		----------<<<<<<	LOADING DATA FROM THE SOURCE TO BRONZE LAYER	>>>>>>----------


		------================= LOADING CRM DATA FROM THE SOURCE =================------

		----***** Bulk load [bronze.crm_cust_info] *****----

		print('-----<<<< Beginning Loading from the Core >>>>------')

		set @start_time = getdate();
		----------------------------
		truncate table bronze.crm_cust_info; --TRUNCATE: Quickly deletet all rows from a table, 
											 --resetting it to an empty state.
		print('============================================');
		print('load the data into [bronze.crm_cust_info]');
		print('============================================');

		Bulk Insert bronze.crm_cust_info
		from 'C:\Users\oamee\OneDrive\#Data Courses\sql_data_warehosue_project\datasets\source_crm\cust_info.csv'
		with (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK					--This paramater locks the table, so it is reserved for this operation 
										--[IT IS GOOD FOR IMPROVING PERFORMANCE]
			);
		----------------------------
		set @end_time = getdate();
		set @duration = convert(varchar, DATEADD(ms, DATEDIFF(ms, @start_time, @end_time), 0), 114);

		print 'Loading starts at: ' + cast(@start_time as varchar)
		print 'Loading ends at: ' + cast(@end_time as varchar)
		print 'execution time: ' + cast(@duration as varchar)

		print(''); --Empty Line

		----***** Bulk load [bronze.crm_prd_info] *****----

		set @start_time = getdate();
		----------------------------
		truncate table bronze.crm_prd_info; --TRUNCATE: Quickly deletet all rows from a table, 
											--resetting it to an empty state.
		print('============================================');
		print('load the data into [bronze.crm_prd_info]');
		print('============================================');

		Bulk Insert bronze.crm_prd_info
		from 'C:\Users\oamee\OneDrive\#Data Courses\sql_data_warehosue_project\datasets\source_crm\prd_info.csv'
		with (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK					--This paramater locks the table, so it is reserved for this operation 
										--[IT IS GOOD FOR IMPROVING PERFORMANCE]
			);
		----------------------------
		set @end_time = getdate();
		set @duration = convert(varchar, DATEADD(ms, DATEDIFF(ms, @start_time, @end_time), 0), 114);

		print 'Loading starts at: ' + cast(@start_time as varchar)
		print 'Loading ends at: ' + cast(@end_time as varchar)
		print 'execution time: ' + cast(@duration as varchar)

		print(''); --Empty Line



		----***** Bulk load [bronze.crm_sales_details] *****----

		set @start_time = getdate();
		----------------------------
		truncate table bronze.crm_sales_details; --TRUNCATE: Quickly deletet all rows from a table, 
												 --resetting it to an empty state.
		print('============================================');
		print('load the data into [bronze.crm_sales_details]');
		print('============================================');

		Bulk Insert bronze.crm_sales_details
		from 'C:\Users\oamee\OneDrive\#Data Courses\sql_data_warehosue_project\datasets\source_crm\sales_details.csv'
		with (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK					--This paramater locks the table, so it is reserved for this operation 
										--[IT IS GOOD FOR IMPROVING PERFORMANCE]
			);
		----------------------------
		set @end_time = getdate();
		set @duration = convert(varchar, DATEADD(ms, DATEDIFF(ms, @start_time, @end_time), 0), 114);

		print 'Loading starts at: ' + cast(@start_time as varchar)
		print 'Loading ends at: ' + cast(@end_time as varchar)
		print 'execution time: ' + cast(@duration as varchar)

		print(''); --Empty Line



		------================= LOADING CRM DATA FROM THE SOURCE =================------


		----***** Bulk load [bronze.erp_cust_az12] *****----

		set @start_time = getdate();
		----------------------------
		truncate table bronze.erp_cust_az12; --TRUNCATE: Quickly deletet all rows from a table, 
											 --resetting it to an empty state.
		print('============================================');
		print('load the data into [bronze.erp_cust_az12]');
		print('============================================');

		Bulk Insert bronze.erp_cust_az12
		from 'C:\Users\oamee\OneDrive\#Data Courses\sql_data_warehosue_project\datasets\source_erp\CUST_AZ12.csv'
		with (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK					--This paramater locks the table, so it is reserved for this operation 
										--[IT IS GOOD FOR IMPROVING PERFORMANCE]
			);
		----------------------------
		set @end_time = getdate();
		set @duration = convert(varchar, DATEADD(ms, DATEDIFF(ms, @start_time, @end_time), 0), 114);

		print 'Loading starts at: ' + cast(@start_time as varchar)
		print 'Loading ends at: ' + cast(@end_time as varchar)
		print 'execution time: ' + cast(@duration as varchar)

		print(''); --Empty Line


		----***** Bulk load [bronze.erp_loc_a101] *****----

		set @start_time = getdate();
		----------------------------
		truncate table bronze.erp_loc_a101; --TRUNCATE: Quickly deletet all rows from a table, 
											--resetting it to an empty state.
		print('============================================');
		print('load the data into [bronze.erp_loc_a101]');
		print('============================================');

		Bulk Insert bronze.erp_loc_a101
		from 'C:\Users\oamee\OneDrive\#Data Courses\sql_data_warehosue_project\datasets\source_erp\LOC_A101.csv'
		with (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK					--This paramater locks the table, so it is reserved for this operation 
										--[IT IS GOOD FOR IMPROVING PERFORMANCE]
			);
		----------------------------
		set @end_time = getdate();
		set @duration = convert(varchar, DATEADD(ms, DATEDIFF(ms, @start_time, @end_time), 0), 114);

		print 'Loading starts at: ' + cast(@start_time as varchar)
		print 'Loading ends at: ' + cast(@end_time as varchar)
		print 'execution time: ' + cast(@duration as varchar)

		print(''); --Empty Line



		----***** Bulk load [bronze.erp_px_cat_g1v2] *****----

		set @start_time = getdate();
		----------------------------
		truncate table bronze.erp_px_cat_g1v2; --TRUNCATE: Quickly deletet all rows from a table, 
											--resetting it to an empty state.
		print('============================================');
		print('load the data into [bronze.erp_px_cat_g1v2]');
		print('============================================');

		Bulk Insert bronze.erp_px_cat_g1v2
		from 'C:\Users\oamee\OneDrive\#Data Courses\sql_data_warehosue_project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK					--This paramater locks the table, so it is reserved for this operation 
										--[IT IS GOOD FOR IMPROVING PERFORMANCE]
			);
			----------------------------
		set @end_time = getdate();
		set @duration = convert(varchar, DATEADD(ms, DATEDIFF(ms, @start_time, @end_time), 0), 114);

		print 'Loading starts at: ' + cast(@start_time as varchar)
		print 'Loading ends at: ' + cast(@end_time as varchar)
		print 'execution time: ' + cast(@duration as varchar)


	end try

	begin catch

		print '=======================================================';
		print 'ERROR OCCURED DURING LOADING THE DATA INTO BRONZE LAYER';
		print 'Error Message: ' + ERROR_MESSAGE();
		print 'Error Number: ' + CAST( ERROR_NUMBER() AS NVARCHAR);
		print 'Error State: ' + CAST( ERROR_STATE() AS NVARCHAR);

	end catch

set @batch_end_time = getdate();
set @batch_duration = convert(varchar, DATEADD(ms, DATEDIFF(ms, @batch_start_time, @batch_end_time), 0), 114);

print('-------------------------------------------------------');
print('The execution time of the procedure:');
print 'starts at: ' + cast(@start_time as varchar);
print 'ends at: ' + cast(@end_time as varchar);
print 'execution time: ' + cast(@duration as varchar);


end
GO

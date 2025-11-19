/*
 
 DDL Script: Create Bronze Tables
 
 Script Purpose:
 This script creates tables in the 'bronze' schema, dropping existing tables
 if they already exist.
 Run this script to re-define the DDL structure of 'bronze' Tables
 ==============================================================================
 */
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
go
create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);


IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;


create table bronze.crm_prd_info(
prd_id		 int,
prd_key		 nvarchar(50),
prd_nm		 nvarchar(50),
prd_cost	 int,
prd_line	 nvarchar(50),
prd_start_dt datetime,
prd_end_dt   datetime
);

IF OBJECT_ID ('bronze.crm_sale_details', 'U') IS NOT NULL
DROP TABLE bronze.crm_sale_details;

create table bronze.crm_sale_details(
sls_ord_num   nvarchar(50),
sls_prd_key   nvarchar(50),
sls_cust_id   int,
sls_order_dt  int,
sls_ship_dt   int,
sls_due_dt    int,
sls_sales     int,
sls_quantity  int,
sls_price     int
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;


create table bronze.erp_loc_a101(
cid    nvarchar(50),
cntry  nvarchar(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;

create table bronze.erp_cust_az12(
cid    nvarchar(50),
bdate  date,
gen    nvarchar(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE bronze.erp_px_cat_g1v2;


create table bronze.erp_px_cat_g1v2(
id			 nvarchar(50),
cat			 nvarchar(50),
subcat		 nvarchar(50),
maintenance  nvarchar(50)
);
exec bronze.load_bronze;
go

create or alter procedure bronze.load_bronze
as
begin
    declare 
        @start_time datetime, 
        @end_time datetime,
        @batch_start_time datetime,
        @batch_end_time datetime;

    begin try
        
        set @batch_start_time = getdate();

        print '=========================================================';
        print '                 loading bronze layer                    ';
        print '=========================================================';

        print '---------------------------------------------------------';
        print '                   loading crm tables                    ';
        print '---------------------------------------------------------';

        ---------------------------------------------------------
        -- crm customer info
        ---------------------------------------------------------
        set @start_time = getdate();

        print '>> truncating table: bronze.crm_cust_info';
        truncate table bronze.crm_cust_info;

        print '>> inserting data into: bronze.crm_cust_info';
        bulk insert bronze.crm_cust_info
        from 'F:\New folder\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );

        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> -----------------------------';


        ---------------------------------------------------------
        -- crm product info
        ---------------------------------------------------------
        set @start_time = getdate();

        print '>> truncating table: bronze.crm_prd_info';
        truncate table bronze.crm_prd_info;

        print '>> inserting data into: bronze.crm_prd_info';
        bulk insert bronze.crm_prd_info
        from 'F:\New folder\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );

        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> -----------------------------';


        ---------------------------------------------------------
        -- crm sales details
        ---------------------------------------------------------
        set @start_time = getdate();

        print '>> truncating table: bronze.crm_sale_details';
        truncate table bronze.crm_sale_details;

        print '>> inserting data into: bronze.crm_sale_details';
        bulk insert bronze.crm_sale_details
        from 'F:\New folder\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );

        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> -----------------------------';



        print '---------------------------------------------------------';
        print '                   loading erp tables                    ';
        print '---------------------------------------------------------';

        ---------------------------------------------------------
        -- erp customer
        ---------------------------------------------------------
        set @start_time = getdate();

        print '>> truncating table: bronze.erp_cust_az12';
        truncate table bronze.erp_cust_az12;

        print '>> inserting data into: bronze.erp_cust_az12';
        bulk insert bronze.erp_cust_az12
        from 'F:\New folder\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );

        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> -----------------------------';


        ---------------------------------------------------------
        -- erp location
        ---------------------------------------------------------
        set @start_time = getdate();

        print '>> truncating table: bronze.erp_loc_a101';
        truncate table bronze.erp_loc_a101;

        print '>> inserting data into: bronze.erp_loc_a101';
        bulk insert bronze.erp_loc_a101
        from 'F:\New folder\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );

        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> -----------------------------';


        ---------------------------------------------------------
        -- erp product category
        ---------------------------------------------------------
        set @start_time = getdate();

        print '>> truncating table: bronze.erp_px_cat_g1v2';
        truncate table bronze.erp_px_cat_g1v2;

        print '>> inserting data into: bronze.erp_px_cat_g1v2';
        bulk insert bronze.erp_px_cat_g1v2
        from 'F:\New folder\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );

        set @end_time = getdate();
        print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
        print '>> -----------------------------';


        ---------------------------------------------------------
        -- final total batch timing
        ---------------------------------------------------------
        set @batch_end_time = getdate();

        print '=========================================================';
        print 'loading bronze layer is completed';
        print ' - total load duration: ' 
              + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar)
              + ' seconds';
        print '=========================================================';

    end try


    begin catch

        print '=========================================================';
        print 'error occurred during loading bronze layer';
        print 'error message: ' + error_message();
        print 'error number : ' + cast(error_number() as nvarchar);
        print 'error state  : ' + cast(error_state() as nvarchar);
        print '=========================================================';

    end catch

end
go

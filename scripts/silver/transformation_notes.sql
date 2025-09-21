/*
=====================================================================================
Sript Purpose:
This script file is a history of all data transformations done on the silver tables and include a detailed notes of the process of transformation
=====================================================================================
*/

----Data Transformation & Loading Silver Tables Notes


----------------------bronze.crm_cust_info-----------------------
	--Check for nulls or duplicates in primary key ===========Expectation: No result
	select count(distinct cst_id),count(*) from bronze.crm_cust_info

	select cst_id, count(*) from bronze.crm_cust_info
	group by cst_id 
	having count(*) >1 or cst_id is null

	--check for unwanted spaces in columns ======Expectation: No Results
	select cst_firstname,trim(cst_firstname) first_name,cst_lastname,trim(cst_lastname) last_name
	from bronze.crm_cust_info 
	where cst_firstname != trim(cst_firstname) or cst_lastname != trim(cst_lastname)

	---Data Standardization & Consistency check =========Expectation: inputs should be meaningful
	select distinct cst_marital_status from bronze.crm_cust_info
	select distinct cst_gndr from bronze.crm_cust_info

	/* Data Transformations Performed
	Data Normalization & Standardization: Mapping coded values to meaningful, user-friendly descriptions
	Remove Unwanted Spaces: removes unwanted spaces to ensure data consistency
	Handling Missing Dtaa: fill in the blanks by adding default value
	Remove duplicates: Ensure only one record for each entity/row by identifying and retaining the most relevant row
	*/

	print '>> Truncating Table: silver.crm_cust_info ';
	truncate table silver.crm_cust_info

	print '>> Inserting Data Into: silver.crm_cust_info ';
	insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	select cst_id, cst_key, trim(cst_firstname) cst_firstname, trim(cst_lastname) cst_lastname
	,case when trim(cst_marital_status) = 'S' then 'Single' when trim(cst_marital_status) = 'M' then 'Married' else 'N/A' end cst_marital_status
	,case when trim(cst_gndr) = 'F' then 'Female' when trim(cst_gndr) = 'M' then 'Male' else 'N/A' end cst_gndr
	,cst_create_date
	from (
		select *,row_number()over(partition by cst_id order by cst_create_date desc) last_input 
		from bronze.crm_cust_info
		where cst_id is not null
	) x
	where last_input = 1 


----------------------bronze.crm_prd_info-----------------------
	--Check for nulls or duplicates in primary key ===========Expectation: No result
	select count(distinct prd_id),count(*) from bronze.crm_prd_info

	select prd_id, count(*) from bronze.crm_prd_info
	group by prd_id 
	having count(*) >1 or prd_id is null

	--check for unwanted spaces in columns ======Expectation: No Results
	select prd_nm,trim(prd_nm) first_name
	from bronze.crm_prd_info 
	where prd_nm != trim(prd_nm)

	--check for null or negative numbers =======Expectation: No Results
	select prd_cost
	from bronze.crm_prd_info 
	where prd_cost < 0 or prd_cost is null

	---Data Standardization & Consistency check =========Expectation: inputs should be meaningful
	select distinct prd_line from bronze.crm_prd_info

	--Check for Invalid Date Orders
	select * from bronze.crm_prd_info
	where prd_start_dt > prd_end_dt
	/*
		there must always be a start date
		end date must be higher than start date
		end date must be lower than next start date of the same product
		end date = start date of the 'next' record - 1
	*/

	/* Data Transformations Performed
	Derived Columns: create new columns based on calculations or transformation of existing columns e.g cat_id
	Handling Missing Dtaa: fill in the blanks by adding default value
	Data Normalization & Standardization: Mapping coded values to meaningful, user-friendly descriptions
	Data type casting
	Data Enrichment: Add new, relevant data to enhance the dataset for analysis
	*/

	print '>> Truncating Table: silver.crm_prd_info ';
	truncate table silver.crm_prd_info

	print '>> Inserting Data Into: silver.crm_prd_info ';
	insert into silver.crm_prd_info( prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	select prd_id, replace(substring(prd_key,1,5),'-','_') cat_id
	,substring(prd_key,7,len(prd_key)) prd_key
	, prd_nm, isnull(prd_cost,0) prd_cost
	,case trim(prd_line) 
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'N/A'
	end	prd_line
	, cast(prd_start_dt as date) prd_start_dt, cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) prd_end_dt
	from bronze.crm_prd_info
	

----------------------bronze.crm_sales_details-----------------------
	--Check for Invalid Date or date outliers
	select * , nullif(sls_order_dt,0)sls_order_dt from bronze.crm_sales_details
	--where sls_order_dt <= 0  or sls_ship_dt <= 0 or sls_due_dt <= 0 
	--where len(sls_order_dt)<> 8 or len(sls_ship_dt)<> 8 or len(sls_due_dt)<> 8
	where sls_order_dt > 20500101 or sls_order_dt < 19000101

	--Check for Invalid Date Orders or date outliers
	select *  from bronze.crm_sales_details
	where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt 

	--Check for business rules
	select *  from bronze.crm_sales_details
	where sls_sales <> sls_quantity * sls_price
	or sls_sales is null or sls_quantity is null or sls_price is null
	or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
	order by sls_sales,sls_quantity,sls_price

	/*
	Business Rules
	sales = quantity * price
	all sales, qty & price must be positive. no negative, nulls or zero
	Bad data issues like incorrect figures can be cleaned from the source system
	if sales is negative,zero or null, use sales formula
	if price is wrong, calculate it from sales & quantity
	if price is negative, convert it to positive
	*/

	/* Data Transformations Performed
	Handling invalid data
	Data type casting
	Handling Missing Dtaa: fill in the blanks by adding default value. This was handled by deriving the data from calculations
	*/

	print '>> Truncating Table: silver.crm_sales_details ';
	truncate table silver.crm_sales_details

	print '>> Inserting Data Into: silver.crm_sales_details ';
	insert into silver.crm_sales_details( sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	select sls_ord_num, sls_prd_key, sls_cust_id
	,case 
		when sls_order_dt = 0 or len(sls_order_dt) <> 8 then null
		else cast(cast(sls_order_dt as varchar) as date)
	end sls_order_dt
	,case 
		when sls_ship_dt = 0 or len(sls_ship_dt) <> 8 then null
		else cast(cast(sls_ship_dt as varchar) as date)
	end sls_ship_dt
	,case 
		when sls_due_dt = 0 or len(sls_due_dt) <> 8 then null
		else cast(cast(sls_due_dt as varchar) as date)
	end sls_due_dt
	,case 
		when sls_sales is null or sls_sales <= 0 or sls_sales <> sls_quantity * sls_price 
		then abs(sls_quantity * sls_price) 
		else sls_sales 
	end sls_sales
	, sls_quantity
	,case 
		when sls_price is null or sls_price <= 0 
		then sls_sales / nullif(sls_quantity,0) 
		else abs(sls_price) 
	end sls_price
	from bronze.crm_sales_details 


----------------------bronze.erp_cust_az12-----------------------

	select *,len(cid) from bronze.erp_cust_az12
	select * from silver.crm_cust_info

	----identify out of range date
	select * from bronze.erp_cust_az12 where bdate < '1924-01-01' or bdate > getdate()

	---Data Standardization & Consistency check =========Expectation: inputs should be meaningful
	select distinct gen from bronze.erp_cust_az12

	/* Data Transformations Performed
	Removed invalid values
	Data normalization
	Handling Missing Dtaa: fill in the blanks by adding default value. This was handled by deriving the data from calculations
	*/

	print '>> Truncating Table: silver.erp_cust_az12 ';
	truncate table silver.erp_cust_az12

	print '>> Inserting Data Into: silver.erp_cust_az12 ';
	insert into silver.erp_cust_az12( cid, bdate, gen)
	select case when cid like 'NAS%' then substring(cid,4,len(cid)) else cid end cid
	,case when bdate > getdate() then null else bdate end bdate
	,case
		when trim(gen) in ('F','Female') then 'Female'
		when trim(gen) in ('M','Male') then 'Male'
		else 'N/A'
	end gen
	from bronze.erp_cust_az12

	


----------------------bronze.erp_cust_az12-----------------------

	select * from bronze.erp_loc_a101

	select * from silver.crm_cust_info
	---Data Standardization & Consistency check =========Expectation: inputs should be meaningful
	select distinct cntry from bronze.erp_loc_a101 order by cntry

	/* Data Transformations Performed
		Removed invalid values
		Data normalization 
		Handling Missing Dtaa: fill in the blanks by adding default value. 
		Removed unwanted spaces
	*/

	print '>> Truncating Table: silver.erp_loc_a101 ';
	truncate table silver.erp_loc_a101

	print '>> Inserting Data Into: silver.erp_loc_a101 ';
	insert into silver.erp_loc_a101(cid, cntry)
	select replace(cid,'-','')cid
	,case when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('United States','US','USA') then 'United States of America'
		when trim(cntry) = '' or cntry is null then 'N/A'
		else trim(cntry)
	end cntry
	from bronze.erp_loc_a101

	
----------------------bronze.erp_cust_az12-----------------------

	---check for unwanted spaces
	select * from bronze.erp_px_cat_g1v2
	where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

	---Data Standardization & Consistency check =========Expectation: inputs should be meaningful
	select distinct cat from bronze.erp_px_cat_g1v2
	select distinct subcat from bronze.erp_px_cat_g1v2
	select distinct maintenance from bronze.erp_px_cat_g1v2


	print '>> Truncating Table: silver.erp_px_cat_g1v2 ';
	truncate table silver.erp_px_cat_g1v2

	print '>> Inserting Data Into: silver.erp_px_cat_g1v2 ';
	insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	select * from bronze.erp_px_cat_g1v2






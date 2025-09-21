/*
=====================================================================================
Sript Purpose:
This script performs various quality checks to ensure that the transformed data is accurate and consistent. It includes checks for:
--Duplicate or null primary keys
--Unwanted Spaces
-Data Standardization & Normalization
-Invalid Dates or date range
=====================================================================================
*/

----Silver Quality Check
-------crm_cust_info---------------
	--Check for nulls or duplicates in primary key =========Expectation: No result
	select count(distinct cst_id),count(*) from silver.crm_cust_info

	select cst_id, count(*) from silver.crm_cust_info
	group by cst_id 
	having count(*) >1 or cst_id is null

	--check for unwanted spaces in columns ==============Expectation: No Results
	select cst_firstname,trim(cst_firstname) first_name,cst_lastname,trim(cst_lastname) last_name
	from silver.crm_cust_info
	where cst_firstname != trim(cst_firstname) or cst_lastname != trim(cst_lastname)

	---Data Standardization & Consistency check ============Expectation: inputs should be meaningful
	select distinct cst_marital_status from silver.crm_cust_info
	select distinct cst_gndr from silver.crm_cust_info

-------crm_prd_info-----------------------------
	--Check for nulls or duplicates in primary key =================Expectation: No result
	select count(distinct prd_id),count(*) from silver.crm_prd_info

	select prd_id, count(*) from silver.crm_prd_info
	group by prd_id 
	having count(*) >1 or prd_id is null

	--check for unwanted spaces in columns ================Expectation: No Results
	select prd_nm,trim(prd_nm) first_name
	from silver.crm_prd_info 
	where prd_nm != trim(prd_nm)

	--check for null or negative numbers ===============Expectation: No Results
	select prd_cost
	from silver.crm_prd_info 
	where prd_cost < 0 or prd_cost is null

	---Data Standardization & Consistency check =============Expectation: inputs should be meaningful
	select distinct prd_line from silver.crm_prd_info


	--Check for Invalid Date Orders
	select * from silver.crm_prd_info
	where prd_start_dt > prd_end_dt

-------crm_sales_details-----------------------------
	--Check for Invalid Date or date outliers
	select * , nullif(sls_order_dt,0)sls_order_dt from silver.crm_sales_details
	--where sls_order_dt <= 0  or sls_ship_dt <= 0 or sls_due_dt <= 0 
	--where len(sls_order_dt)<> 8 or len(sls_ship_dt)<> 8 or len(sls_due_dt)<> 8
	--where sls_order_dt > 20500101 or sls_order_dt < 19000101

	--Check for Invalid Date Orders or date outliers
	select *  from silver.crm_sales_details
	where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt 

		--Check for business rules
	select *  from silver.crm_sales_details
	where sls_sales <> sls_quantity * sls_price
	or sls_sales is null or sls_quantity is null or sls_price is null
	or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
	order by sls_sales,sls_quantity,sls_price


-------erp_cust_az12-----------------------------
	----identify out of range date
	select * from silver.erp_cust_az12 where bdate < '1924-01-01' or bdate > getdate()

	---Data Standardization & Consistency check =========Expectation: inputs should be meaningful
	select distinct gen from silver.erp_cust_az12

-------erp_loc_a101-----------------------------
	---Data Standardization & Consistency check =========Expectation: inputs should be meaningful
	select distinct cntry from silver.erp_loc_a101 order by cntry


-------erp_px_cat_g1v2-----------------------------

	---check for unwanted spaces
	select * from silver.erp_px_cat_g1v2
	where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

	---Data Standardization & Consistency check =========Expectation: inputs should be meaningful
  select distinct cat from silver.erp_px_cat_g1v2
  select distinct subcat from silver.erp_px_cat_g1v2
  select distinct maintenance from silver.erp_px_cat_g1v2

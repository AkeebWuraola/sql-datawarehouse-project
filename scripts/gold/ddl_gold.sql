/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

/* Tips:
	--check if there are duplicates as a result of the join. use a subquery and count
	--Further checks reveal that the gender in the crm table and erp table are not matching for each customers, you need to determine which is master and perform some transformation where applicable in case you have to combine the two columns
	--arrange your columns
*/
create view gold.dim_customers as 

select row_number() over(order by cst_id) customer_key, a.cst_id customer_id,a.cst_key customer_number,a.cst_firstname first_name,a.cst_lastname last_name
,case when a.cst_gndr !='n/a' then a.cst_gndr else isnull(b.gen,'N/A') end gender,a.cst_marital_status marital_status, c.cntry country
, b.bdate date_of_birth, a.cst_create_date customer_creation_date
from silver.crm_cust_info a
left join silver.erp_cust_az12 b on a.cst_key = b.cid
left join silver.erp_loc_a101 c on a.cst_key = c.cid


/*
	get only current data
	Check for duplicates as a result of the join. do a subquery on the primary key
	Rename columns to friendly, meaningful names
*/
create view gold.dim_products as 

select row_number() over(order by prd_start_dt,prd_key ) product_key,prd_id product_id, prd_key product_number, prd_nm product_name, cat_id category_id, b.cat category, b.subcat sub_category, b.maintenance, prd_cost product_cost, prd_line product_line, prd_start_dt start_date
from silver.crm_prd_info a
left join silver.erp_px_cat_g1v2 b on a.cat_id = b.id
where prd_end_dt is null


/* 
	In creating the fact table, you have to bring the surrogate key into the fact table and exclude the primary key
*/
create view gold.fact_sales as 

select sls_ord_num order_number, b.product_key, c.customer_key, sls_order_dt order_date, sls_ship_dt shipping_date, sls_due_dt due_date,sls_sales sales_amount, sls_quantity quantity, sls_price price
from silver.crm_sales_details a
left join gold.dim_products b on a.sls_prd_key = b.product_number
left join gold.dim_customers c on a.sls_cust_id = c.customer_id


/*
	Check if all dimension tables can successfully join to the fact table
*/

select * 
from gold.fact_sales a
left join gold.dim_customers b on a.customer_key = b.customer_key
left join gold.dim_products c on a.product_key = c.product_key
--where b.customer_key is null
where c.product_key is null

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT customer_key, COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT product_key,  COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  

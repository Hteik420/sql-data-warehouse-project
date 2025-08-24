Print 'Silver CRM Sales Quality Check'
Print '----------------------------------'
-----------------------------------------------------------
-- Check for Invalid Dates
SELECT
NULLIF (sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

SELECT
NULLIF (sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101

SELECT
NULLIF (sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101

-- Check for Invalid Date Orders
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative. 

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Check the quality of the silver table 
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT DISTINCT 
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details

Print 'Silver Customer Info Quality Check'
Print '----------------------------------'

SELECT cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT
*
FROM bronze.crm_cust_info
WHERE cst_id=29466

-- Check for unwanted Spaces
-- Expectation: No Results
-- If there are results it means the original name is not equal to the cleaned version (means it has extra spaces)
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT * FROM silver.crm_cust_info

Print 'Silver Customer Info Quality Check'
Print '----------------------------------'

USE DataWarehouse
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT distinct id from bronze.erp_px_cat_g1v2

SELECT sls_prd_key FROM bronze.crm_sales_details

-- Check Unwated spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm  != TRIM (prd_nm)

-- CHeck for NULLs or Negative Numbers
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

----------------------------------------------
-- Playing around with the queries to check if there is any missing data, unwanted spaces
-- You can replace silver and bronze interchangbly to check

USE DataWarehouse
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT distinct id from silver.erp_px_cat_g1v2
SELECT sls_prd_key FROM silver.crm_sales_details

-- Check Unwated spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm  != TRIM (prd_nm)

-- CHeck for NULLs or Negative Numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT * 
FROM silver.crm_prd_info

Print 'Silver erp_cust Quality Check'
Print '----------------------------------'

-----------------------------------------------------------------------------------------------------------------------------------------
-- Identify Out-of_Range Dates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE() -- can report it to source system to correct it since the birthdays don't make sense 

-- Data Standardization & Consistency
USE DataWarehouse
SELECT * FROM bronze.erp_cust_az12

SELECT DISTINCT gen,
CASE 
    WHEN gen IS NULL OR REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '') = '' THEN 'n/a'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('M','MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen_cleaned
FROM bronze.erp_cust_az12
-----------------------------------------------------------------------------------------------------------------------------------------
-- Check the data quality in the silver layer
SELECT DISTINCT
gen
FROM silver.erp_cust_az12

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

SELECT * FROM silver.erp_cust_az12

Print 'Silver erp_loc Quality Check'
Print '----------------------------------'

-- Data Standardization & Consistency
SELECT DISTINCT 
    CNTRY AS old_cntry,
    CASE 
        WHEN REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '') = 'DE' THEN 'Germany'
        WHEN REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '') IN ('US', 'USA') THEN 'United States'
        WHEN cntry IS NULL OR REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '') = '' THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry_cleaned
FROM bronze.erp_loc_a101
ORDER BY cntry;
-----------------------------------------------------------------------------
-- Validate the data after loading

SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY CNTRY
SELECT * FROM silver.erp_loc_a101

Print 'Silver erp_px cat Quality Check'
Print '----------------------------------'

--------------------------------------------------------------------------------
-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
    CASE 
        WHEN maintenance IS NULL OR 
             LTRIM(RTRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), ''))) = '' THEN 'n/a'
        ELSE LTRIM(RTRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), '')))
    END AS maintenance
FROM bronze.erp_px_cat_g1v2;
--------------------------------------------------------------------------------
SELECT * FROM silver.erp_px_cat_g1v2


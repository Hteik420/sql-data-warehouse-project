EXEC silver.load_silver

USE DataWarehouse
GO 

CREATE OR ALTER PROCEDURE silver.load_silver
AS 
BEGIN

-- Loading silver.crm_cust_info
PRINT '>>Truncating Table: silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' Then 'Single'
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     ELSE 'Unknown'
END AS cst_marital_status,   
CASE 
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'Unknown'
END AS cst_gndr,
cst_create_date
FROM (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last =1

-- Loading crm_sales
PRINT '>>Truncating Table: silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) --Change from varchar to date
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) --Change from varchar to date
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) --Change from varchar to date
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

PRINT '>>Truncating Table: silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12;
-- Loading data (after transforming)
INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
-- Clean and transform data 
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
    ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
END AS bdate,
CASE 
    WHEN gen IS NULL OR REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '') = '' THEN 'n/a'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('M','MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

PRINT '>>Truncating Table: silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 --Load the below transformed data
(cid,cntry)
-----------------------------------------------------------------------------
-- Transform data
SELECT
REPLACE (cid,'-','') cid, --SELECT cst_key FROM silver.crm_cust_info; This cst_key must match. Check the architecture. 
CASE 
    WHEN REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '') = 'DE' THEN 'Germany'
    WHEN REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '') IN ('US', 'USA') THEN 'United States'
    WHEN cntry IS NULL OR REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '') = '' THEN 'n/a'
    ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101 

PRINT '>>Truncating Table: silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
SELECT
id,
cat,
subcat,
    CASE 
        WHEN maintenance IS NULL OR 
             LTRIM(RTRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), ''))) = '' THEN 'n/a'
        ELSE LTRIM(RTRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), '')))
    END AS maintenance
FROM bronze.erp_px_cat_g1v2

PRINT '>>Truncating Table: silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
prd_id,
REPLACE (SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,--to extract a certain part of the string
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL (prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
     WHEN 'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'other Sales'
     WHEN 'T' THEN 'Touring'
     Else 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt_test
FROM bronze.crm_prd_info
END
GO
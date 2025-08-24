-- Update the ddl afte the below loading script
IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
----------------------------------------------
-- Loading prd info into silver table
USE DataWarehouse
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



----------------------------------------------
-- Playing around with the queries to check if there is any missing data, unwanted spaces
-- You can replace silver and bronze interchangbly to check
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
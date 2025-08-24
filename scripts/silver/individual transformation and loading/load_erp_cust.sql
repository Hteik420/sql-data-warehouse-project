USE DataWarehouse
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

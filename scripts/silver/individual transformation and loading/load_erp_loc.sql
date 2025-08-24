USE DataWarehouse
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
-----------------------------------------------------------------------------
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


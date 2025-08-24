USE DataWarehouse
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
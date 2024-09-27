CREATE   VIEW [tickets].[vProduct]
AS
WITH ProductCategories AS (
    SELECT
        s.value AS ProductID,
        pim.LocalizedLabel AS ProductName,
        CASE
            WHEN UPPER(pim.LocalizedLabel) LIKE '%PORTABLE SAWMILL%' THEN 'Sawmills'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%SAWMILL%' THEN 'Sawmills'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%WOODLANDER%' THEN 'Sawmills'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%WOOD CHIPPER%' THEN 'Wood Chippers'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%STUMP GRINDER%' THEN 'Stump Grinder'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%BUSHLANDER TRAILER%' THEN 'Sawmills'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%TRACK EXTENSION%' THEN 'Sawmills'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%TRAILER EXTENSION%' THEN 'Sawmills'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%MULTILANDER%' THEN 'Utility Trailer'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%T-REX/WMT-01%' THEN 'Utility Trailer'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%FIREWOOD PROCESSOR%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%PC-8M/H%' THEN 'Wood Chippers'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%SAWMILL BLADES%' THEN 'Sawmills'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%BLADE SHARPENER%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%BLADE TOOTH SETTER%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%OTHER ACCESSORY%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%DO NOT USE%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%LOGLANDER 20%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%LOGLANDER 28%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%MERCHANDISE%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%PATHLANDER%' THEN 'Utility Trailer'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%TF810 WOOD CHIPPER%' THEN 'Wood Chippers'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%RS30 PRO%' THEN 'Other'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%POWER HEAD 110%' THEN 'Sawmills'
            WHEN UPPER(pim.LocalizedLabel) LIKE '%WG28 PRO STUMP GRINDER%' THEN 'Stump Grinder'
            ELSE 'Other'
        END AS ProductCategory
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Incident] i
    CROSS APPLY STRING_SPLIT(i.wsi_productsimpacted, ';') s
    LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[OptionsetMetadata] pim
        ON pim.EntityName = 'incident' AND pim.OptionSetName = 'wsi_productsimpacted' AND pim.[Option] = s.value
),
UniqueProducts AS (
    SELECT 
        ProductID,
        ProductName,
        ProductCategory,
        ROW_NUMBER() OVER (PARTITION BY ProductName ORDER BY ProductName) AS RowNum
    FROM ProductCategories
)
SELECT 
    ProductID,
    ProductName,
    ProductCategory
FROM UniqueProducts
WHERE RowNum = 1;
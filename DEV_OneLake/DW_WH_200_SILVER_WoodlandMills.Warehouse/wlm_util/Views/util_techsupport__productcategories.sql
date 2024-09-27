create view "wlm_util"."util_techsupport__productcategories" as with
    src as (
        SELECT 'PORTABLE SAWMILL' AS [productnamelike], 'Sawmills' AS [productcategory]
        UNION ALL
        SELECT 'SAWMILL', 'Sawmills'
        UNION ALL
        SELECT 'WOODLANDER', 'Sawmills'
        UNION ALL
        SELECT 'WOOD CHIPPER', 'Wood Chippers'
        UNION ALL
        SELECT 'STUMP GRINDER', 'Stump Grinder'
        UNION ALL
        SELECT 'BUSHLANDER TRAILER', 'Sawmills'
        UNION ALL
        SELECT 'TRACK EXTENSION', 'Sawmills'
        UNION ALL
        SELECT 'TRAILER EXTENSION', 'Sawmills'
        UNION ALL
        SELECT 'MULTILANDER', 'Utility Trailer'
        UNION ALL
        SELECT 'T-REX/WMT-01', 'Utility Trailer'
        UNION ALL
        SELECT 'FIREWOOD PROCESSOR', 'Other'
        UNION ALL
        SELECT 'PC-8M/H', 'Wood Chippers'
        UNION ALL
        SELECT 'SAWMILL BLADES', 'Sawmills'
        UNION ALL
        SELECT 'BLADE SHARPENER', 'Other'
        UNION ALL
        SELECT 'BLADE TOOTH SETTER', 'Other'
        UNION ALL
        SELECT 'OTHER ACCESSORY', 'Other'
        UNION ALL
        SELECT 'DO NOT USE', 'Other'
        UNION ALL
        SELECT 'LOGLANDER 20', 'Other'
        UNION ALL
        SELECT 'LOGLANDER 28', 'Other'
        UNION ALL
        SELECT 'MERCHANDISE', 'Other'
        UNION ALL
        SELECT 'PATHLANDER', 'Utility Trailer'
        UNION ALL
        SELECT 'TF810 WOOD CHIPPER', 'Wood Chippers'
        UNION ALL
        SELECT 'RS30 PRO', 'Other'
        UNION ALL
        SELECT 'POWER HEAD 110', 'Sawmills'
        UNION ALL
        SELECT 'WG28 PRO STUMP GRINDER', 'Stump Grinder'
    )

select *
from src;
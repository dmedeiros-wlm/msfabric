CREATE   VIEW [Silver].[vShippingLocation]
AS
WITH RankedShippingLocation AS (
    SELECT 
        systemId AS ShippingLocationID,
        UPPER(LTRIM(RTRIM(code))) AS ShippingLocationCode,
        UPPER(LTRIM(RTRIM(name))) AS ShippingLocationName,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[VendorLocation]
)
SELECT
    rvv.ShippingLocationID,
    rvv.ShippingLocationCode,
    rvv.ShippingLocationName,
    rvv.CreatedByUserID,
    rvv.CreatedOn,
    rvv.CreatedOnDate,
    rvv.LastUpdatedByUserID,
    rvv.LastUpdated,
    rvv.LastUpdatedDate
FROM RankedShippingLocation rvv
WHERE rvv.rn = 1
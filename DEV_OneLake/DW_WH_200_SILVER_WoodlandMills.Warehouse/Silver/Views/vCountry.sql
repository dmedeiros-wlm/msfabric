CREATE   VIEW [Silver].[vCountry]
AS
WITH RankedCountries AS (
    SELECT 
        systemId AS CountryID,
        UPPER(LTRIM(RTRIM(code))) AS CountryCode,
        UPPER(LTRIM(RTRIM(name))) AS CountryName,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Country]
)
SELECT
    rc.CountryID,
    rc.CountryCode,
    rc.CountryName,
    rc.CreatedByUserID,
    rc.CreatedOn,
    rc.CreatedOnDate,
    rc.LastUpdatedByUserID,
    rc.LastUpdated,
    rc.LastUpdatedDate
FROM RankedCountries rc
WHERE rc.rn = 1
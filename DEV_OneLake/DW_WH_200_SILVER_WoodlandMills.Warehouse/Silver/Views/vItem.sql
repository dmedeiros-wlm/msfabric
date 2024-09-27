CREATE   VIEW [Silver].[vItem]
AS
WITH RankedItem AS (
    SELECT 
        systemId AS ItemID,
        UPPER(LTRIM(RTRIM([no]))) AS ItemNmber,
        UPPER(LTRIM(RTRIM(description))) AS ItemDescription,
        UPPER(LTRIM(RTRIM(itemCategoryCode))) AS ItemCategory,
        UPPER(LTRIM(RTRIM(genProdPostingGroup))) AS GenProdPostingGroup,
        UPPER(LTRIM(RTRIM(globalDimension1Code))) AS GlobalDimension1,
        UPPER(LTRIM(RTRIM(globalDimension2Code))) AS GlobalDimension2,
        grossWeight AS GrossWeight,
        unitVolume AS UnitVolume,
        unitCost AS UnitCost,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Item]
)
SELECT
    ri.ItemID,
    ri.ItemNmber,
    ri.ItemDescription,
    ri.ItemCategory,
    ri.GenProdPostingGroup,
    ri.GlobalDimension1,
    ri.GlobalDimension2,
    ri.GrossWeight,
    ri.UnitVolume,
    ri.UnitCost,
    ri.CreatedByUserID,
    ri.CreatedOn,
    ri.CreatedOnDate,
    ri.LastUpdatedByUserID,
    ri.LastUpdated,
    ri.LastUpdatedDate
FROM RankedItem ri
WHERE ri.rn = 1
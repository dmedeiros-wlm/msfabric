CREATE   VIEW [Silver].[vProduct]
AS
WITH RankedProducts AS (
    SELECT 
        Id AS ProductID,
        UPPER(LTRIM(RTRIM(productnumber))) AS ProductNumber,
        wm_revisionid AS RevisionID,
        wsi_modelclassname AS ModelClass,
        wsi_salestypecode AS SalesType,
        wm_lottracked AS LotTracked,
        wsi_length AS Length,
        wsi_width AS Width,
        wsi_height AS Height,
        wm_packagetype AS PackageType,
        wm_usablecontainervolume AS UsableContainerVolume,
        wm_weightproduct AS WeightProduct,
        wm_unitspercontainer AS UnitsPerContainer,
        wm_unitspercratelength AS UnitsPerCrateLength,
        wm_unitspercratewidth AS UnitsPerCrateWidth,
        wm_unitspercrateheight AS UnitsPerCrateHeight,
        wm_unitspercratetotal AS UnitsPerCrateTotal,
        wm_producttypecode AS ProductType,    
        createdon AS CreatedOn,
        CAST(createdon AS DATE) AS CreatedOnDate,
        createdby AS CreatedByUserID,
        modifiedon AS LastUpdated,
        CAST(modifiedon AS DATE) AS LastUpdatedDate,
        modifiedby AS LastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Product]
)
SELECT
    ProductID,
    ProductNumber,
    RevisionID,
    ModelClass,
    SalesType,
    LotTracked,
    Length,
    Width,
    Height,
    PackageType,
    UsableContainerVolume,
    WeightProduct,
    UnitsPerContainer,
    UnitsPerCrateLength,
    UnitsPerCrateWidth,
    UnitsPerCrateHeight,
    UnitsPerCrateTotal,
    ProductType,    
    CreatedOn,
    CreatedOnDate,
    CreatedByUserID,
    LastUpdated,
    LastUpdatedDate,
    LastUpdatedByUserID
FROM RankedProducts
WHERE rn = 1
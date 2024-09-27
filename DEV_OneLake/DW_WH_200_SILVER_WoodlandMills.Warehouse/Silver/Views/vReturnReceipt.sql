CREATE   VIEW [Silver].[vReturnReceipt]
AS
WITH RankedReturnReceipt AS (
    SELECT 
        systemId AS ReturnReceiptID,
        UPPER(LTRIM(RTRIM([no]))) AS ReturnReceiptNumber,
        UPPER(LTRIM(RTRIM(returnOrderNo))) AS ReturnOrderNumber,
        UPPER(LTRIM(RTRIM(sellToCustomerNo))) AS CustomerNumber,
        UPPER(LTRIM(RTRIM(shipToAddress))) AS ShippedToAddress,
        UPPER(LTRIM(RTRIM(shipToCity))) AS ShippedToCity,
        UPPER(LTRIM(RTRIM(shipToCounty))) AS ShippedToState,
        UPPER(LTRIM(RTRIM(shipToCountryRegionCode))) AS ShippedToCountry,
        UPPER(LTRIM(RTRIM(shipToPostCode))) AS ShippedToPostCode,
        postingDate AS PostingDate,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[ReturnReceipt]
),
RankedReturnReceiptLine AS (
    SELECT 
        systemId AS ReturnReceiptLineID,
        UPPER(LTRIM(RTRIM(documentNo))) AS ReturnReceiptNumber,
        UPPER(LTRIM(RTRIM([no]))) AS ItemNumber,
        quantity AS LineQuantity,
        UPPER(LTRIM(RTRIM(type))) AS LineType,
        UPPER(LTRIM(RTRIM(genProdPostingGroup))) AS LineGenProdPostingGroup,
        UPPER(LTRIM(RTRIM(postingGroup))) AS LinePostingGroup,
        systemCreatedBy AS LineCreatedByUserID,
        systemCreatedAt AS LineCreatedOn,
        CAST(systemCreatedAt AS DATE) AS LineCreatedOnDate,
        systemModifiedBy AS LineLastUpdatedByUserID,
        systemModifiedAt AS LineLastUpdated,
        CAST(systemModifiedAt AS DATE) AS LineLastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[ReturnReceiptLine]
)
SELECT
    rrr.ReturnReceiptID,
    rrr.ReturnReceiptNumber,
    rrr.ReturnOrderNumber,
    rrr.CustomerNumber,
    rrr.ShippedToAddress,
    rrr.ShippedToCity,
    rrr.ShippedToState,
    rrr.ShippedToCountry,
    rrr.ShippedToPostCode,
    rrr.PostingDate,
    rrr.CreatedByUserID,
    rrr.CreatedOn,
    rrr.CreatedOnDate,
    rrr.LastUpdatedByUserID,
    CASE WHEN rrr.[LastUpdated] > rrrl.LineLastUpdated THEN rrr.[LastUpdated] ELSE rrrl.LineLastUpdated END AS LastUpdated,
    CASE WHEN rrr.[LastUpdatedDate] > rrrl.LineLastUpdatedDate THEN rrr.[LastUpdatedDate] ELSE rrrl.LineLastUpdatedDate END AS LastUpdatedDate,
    rrrl.ReturnReceiptLineID,
    rrrl.ItemNumber,
    rrrl.LineQuantity,
    rrrl.LineType,
    rrrl.LineGenProdPostingGroup,
    rrrl.LinePostingGroup,
    rrrl.LineCreatedByUserID,
    rrrl.LineCreatedOn,
    rrrl.LineCreatedOnDate,
    rrrl.LineLastUpdatedByUserID,
    rrrl.LineLastUpdated,
    rrrl.LineLastUpdatedDate
FROM RankedReturnReceipt rrr
LEFT JOIN RankedReturnReceiptLine rrrl ON rrr.ReturnReceiptNumber = rrrl.ReturnReceiptNumber
WHERE rrr.rn = 1 AND rrrl.rn = 1
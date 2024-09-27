CREATE   VIEW [Silver].[vTransferOrder]
AS
WITH RankedTransferOrder AS (
    SELECT 
        systemId AS TransferOrderID,
        UPPER(LTRIM(RTRIM([no]))) AS TransferOrderNumber,
        UPPER(LTRIM(RTRIM(transferFromCode))) AS TransferFromLocation,
        UPPER(LTRIM(RTRIM(transferToCode))) AS TransferToLocation,
        postingDate AS PostingDate,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[TransferOrder]
),
RankedTransferOrderLine AS (
    SELECT 
        systemId AS TransferOrderLineID,
        UPPER(LTRIM(RTRIM(documentNo))) AS TransferOrderNumber,
        UPPER(LTRIM(RTRIM(itemNo))) AS ItemNumber,
        quantity AS LineQuantity,
        qtyToShip AS QuantityToShip,
        qtyToReceive AS QuantityToReceive,
        systemCreatedBy AS LineCreatedByUserID,
        systemCreatedAt AS LineCreatedOn,
        CAST(systemCreatedAt AS DATE) AS LineCreatedOnDate,
        systemModifiedBy AS LineLastUpdatedByUserID,
        systemModifiedAt AS LineLastUpdated,
        CAST(systemModifiedAt AS DATE) AS LineLastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[TransferOrderLine]
)
SELECT
    rto.TransferOrderID,
    rto.TransferOrderNumber,
    rto.TransferFromLocation,
    rto.TransferToLocation,
    rto.PostingDate,
    rto.CreatedByUserID,
    rto.CreatedOn,
    rto.CreatedOnDate,
    rto.LastUpdatedByUserID,
    CASE WHEN rto.[LastUpdated] > rtol.LineLastUpdated THEN rto.[LastUpdated] ELSE rtol.LineLastUpdated END AS LastUpdated,
    CASE WHEN rto.[LastUpdatedDate] > rtol.LineLastUpdatedDate THEN rto.[LastUpdatedDate] ELSE rtol.LineLastUpdatedDate END AS LastUpdatedDate,
    rtol.TransferOrderLineID,
    rtol.ItemNumber,
    rtol.LineQuantity,
    rtol.QuantityToShip,
    rtol.QuantityToReceive,
    rtol.LineCreatedByUserID,
    rtol.LineCreatedOn,
    rtol.LineCreatedOnDate,
    rtol.LineLastUpdatedByUserID,
    rtol.LineLastUpdated,
    rtol.LineLastUpdatedDate
FROM RankedTransferOrder rto
LEFT JOIN RankedTransferOrderLine rtol ON rto.TransferOrderNumber = rtol.TransferOrderNumber
WHERE rto.rn = 1 AND rtol.rn = 1
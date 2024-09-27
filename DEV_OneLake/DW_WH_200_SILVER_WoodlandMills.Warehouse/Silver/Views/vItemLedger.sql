CREATE   VIEW [Silver].[vItemLedger]
AS
WITH RankedItemLedger AS (
    SELECT 
        systemId AS EntryID,
        UPPER(LTRIM(RTRIM(entryNo))) AS EntryNumber,
        postingDate AS PostingDate,
        UPPER(LTRIM(RTRIM(entryType))) AS EntryType,
        UPPER(LTRIM(RTRIM([open]))) AS EntryOpen,
        UPPER(LTRIM(RTRIM(documentType))) AS DocumentType,
        UPPER(LTRIM(RTRIM(documentNo))) AS DocumentNumber,
        UPPER(LTRIM(RTRIM(orderNo))) AS OrderNumber,
        UPPER(LTRIM(RTRIM(itemNo))) AS ItemNumber,
        UPPER(LTRIM(RTRIM(locationCode))) AS ShippingLocation,
        quantity AS Quantity,
        invoicedQuantity AS InvoicedQuantity,
        remainingQuantity AS RemainingQuantity,
        salesAmountActual AS SalesAmountActual,
        costAmountActual AS CostAmountActual,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[ItemLedger]
)
SELECT
    ril.EntryID,
    ril.EntryNumber,
    ril.PostingDate,
    ril.EntryType,
    ril.EntryOpen,
    ril.DocumentType,
    ril.DocumentNumber,
    ril.OrderNumber,
    ril.ItemNumber,
    ril.ShippingLocation,
    ril.Quantity,
    ril.InvoicedQuantity,
    ril.RemainingQuantity,
    ril.SalesAmountActual,
    ril.CostAmountActual,
    ril.CreatedByUserID,
    ril.CreatedOn,
    ril.CreatedOnDate,
    ril.LastUpdatedByUserID,
    ril.LastUpdated,
    ril.LastUpdatedDate
FROM RankedItemLedger ril
WHERE ril.rn = 1
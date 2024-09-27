CREATE   VIEW [Silver].[vPurchaseInvoice]
AS
WITH RankedPurchaseInvoice AS (
    SELECT 
        systemId AS PurchaseInvoiceID,
        UPPER(LTRIM(RTRIM([no]))) AS PurchaseInvoiceNumber,
        UPPER(LTRIM(RTRIM(buyfromVendorNo))) AS VendorNumber,
        UPPER(LTRIM(RTRIM(vendorInvoiceNo))) AS VendorInvoiceNumber,
        UPPER(LTRIM(RTRIM(preAssignedNo))) AS PreAssignedNumber,
        UPPER(LTRIM(RTRIM(locationCode))) AS ShippingLocation,
        postingDate AS PostingDate,
        dueDate AS DueDate,
        currencyCode AS Currency,
        amount AS Amount,
        amountIncludingVAT AS AmountWithTaxes,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[PurchaseInvoice]
),
RankedPurchaseInvoiceLine AS (
    SELECT 
        systemId AS PurchaseInvoiceLineID,
        UPPER(LTRIM(RTRIM(documentNo))) AS PurchaseInvoiceNumber,
        UPPER(LTRIM(RTRIM([no]))) AS ItemNumber,
        UPPER(LTRIM(RTRIM(description))) AS ItemDescription,
        UPPER(LTRIM(RTRIM(type))) AS LineType,
        quantity AS LineQuantity,
        lineAmount AS LineAmount,
        systemCreatedBy AS LineCreatedByUserID,
        systemCreatedAt AS LineCreatedOn,
        CAST(systemCreatedAt AS DATE) AS LineCreatedOnDate,
        systemModifiedBy AS LineLastUpdatedByUserID,
        systemModifiedAt AS LineLastUpdated,
        CAST(systemModifiedAt AS DATE) AS LineLastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[PurchaseInvoiceLine]
)
SELECT
    rpi.PurchaseInvoiceID,
    rpi.PurchaseInvoiceNumber,
    rpi.VendorNumber,
    rpi.VendorInvoiceNumber,
    rpi.PreAssignedNumber,
    rpi.ShippingLocation,
    rpi.PostingDate,
    rpi.DueDate,
    rpi.Currency,
    rpi.Amount,
    rpi.AmountWithTaxes,
    rpi.CreatedByUserID,
    rpi.CreatedOn,
    rpi.CreatedOnDate,
    rpi.LastUpdatedByUserID,
    CASE WHEN rpi.[LastUpdated] > rpil.LineLastUpdated THEN rpi.[LastUpdated] ELSE rpil.LineLastUpdated END AS LastUpdated,
    CASE WHEN rpi.[LastUpdatedDate] > rpil.LineLastUpdatedDate THEN rpi.[LastUpdatedDate] ELSE rpil.LineLastUpdatedDate END AS LastUpdatedDate,
    rpil.PurchaseInvoiceLineID,
    rpil.ItemNumber,
    rpil.ItemDescription,
    rpil.LineType,
    rpil.LineQuantity,
    rpil.LineAmount,
    rpil.LineCreatedByUserID,
    rpil.LineCreatedOn,
    rpil.LineCreatedOnDate,
    rpil.LineLastUpdatedByUserID,
    rpil.LineLastUpdated,
    rpil.LineLastUpdatedDate
FROM RankedPurchaseInvoice rpi
LEFT JOIN RankedPurchaseInvoiceLine rpil ON rpi.PurchaseInvoiceNumber = rpil.PurchaseInvoiceNumber
WHERE rpi.rn = 1 AND rpil.rn = 1
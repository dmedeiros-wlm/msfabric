CREATE   VIEW [Silver].[vCustomer]
AS
WITH RankedCustomer AS (
    SELECT 
        systemId AS CustomerID,
        UPPER(LTRIM(RTRIM([no]))) AS CustomerNumber,
        invAmountsLCY AS InvoicedAmountsLocalCurrency,
        invDiscountsLCY AS InvoicedDiscountsLocalCurrency,
        noOfQuotes AS NumberOfQuotes,
        noOfOrders AS NumberOfOrders,
        noOfReturnOrders AS NumberOfReturnOrders,
        noOfShipToAddresses AS NumberOfShipments,
        noOfPstdCreditMemos AS NumberOfPostedCreditMemos,
        noOfPstdInvoices AS NumberOfPostedInvoices,
        noOfPstdReturnReceipts AS NumberOfPostedReturnReceipts,
        noOfPstdShipments AS NumberOfPostedShipments,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Customer]
)
SELECT
    rc.CustomerID,
    rc.CustomerNumber,
    rc.InvoicedAmountsLocalCurrency,
    rc.InvoicedDiscountsLocalCurrency,
    rc.NumberOfQuotes,
    rc.NumberOfOrders,
    rc.NumberOfReturnOrders,
    rc.NumberOfShipments,
    rc.NumberOfPostedCreditMemos,
    rc.NumberOfPostedInvoices,
    rc.NumberOfPostedReturnReceipts,
    rc.NumberOfPostedShipments,
    rc.CreatedByUserID,
    rc.CreatedOn,
    rc.CreatedOnDate,
    rc.LastUpdatedByUserID,
    rc.LastUpdated,
    rc.LastUpdatedDate
FROM RankedCustomer rc
WHERE rc.rn = 1
CREATE   VIEW [Silver].[vSalesInvoice]
AS
WITH RankedSalesInvoice AS (
    SELECT 
        systemId AS InvoiceID,
        UPPER(LTRIM(RTRIM([no]))) AS InvoiceNumber,
        UPPER(LTRIM(RTRIM(orderNo))) AS OrderNumber,
        postingDate AS InvoicePostingDate,
        UPPER(LTRIM(RTRIM(selltoAddress))) AS SellToAddress,
        UPPER(LTRIM(RTRIM(selltoCity))) AS SellToCity,
        UPPER(LTRIM(RTRIM(selltoCounty))) AS SellToState,
        UPPER(LTRIM(RTRIM(selltoCountryRegionCode))) AS SellToCountry,
        UPPER(LTRIM(RTRIM(selltoPostCode))) AS SellToPostCode,
        UPPER(LTRIM(RTRIM(billtoAddress))) AS BillToAddress,
        UPPER(LTRIM(RTRIM(billtoCity))) AS BillToCity,
        UPPER(LTRIM(RTRIM(billtoCounty))) AS BillToState,
        UPPER(LTRIM(RTRIM(billtoCountryRegionCode))) AS BillToCountry,
        UPPER(LTRIM(RTRIM(billtoPostCode))) AS BillToPostCode,
        cancelled AS InvoiceCancelled,
        UPPER(LTRIM(RTRIM(selltoCustomerNo))) AS CustomerNumber,
        amount AS InvoiceAmount,
        invoiceDiscountValue AS InvoiceDiscount,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SalesInvoice]
),
RankedSalesInvoiceLine AS (
    SELECT 
        systemId AS InvoiceLineID,
        UPPER(LTRIM(RTRIM(documentNo))) AS InvoiceNumber,
        UPPER(LTRIM(RTRIM([no]))) AS ItemNumber,
        quantity AS LineQuantity,
        lineAmount AS LineAmount,
        UPPER(LTRIM(RTRIM(type))) AS LineType,
        UPPER(LTRIM(RTRIM(genProdPostingGroup))) AS LineGenProdPostingGroup,
        UPPER(LTRIM(RTRIM(postingGroup))) AS LinePostingGroup,
        systemCreatedAt AS LineCreatedOn,
        CAST(systemCreatedAt AS DATE) AS LineCreatedOnDate,
        systemModifiedAt AS LineLastUpdated,
        CAST(systemModifiedAt AS DATE) AS LineLastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SalesInvoiceLine]
)
SELECT
    rsi.InvoiceID,
    rsi.InvoiceNumber,
    rsi.OrderNumber,
    rsi.InvoicePostingDate,
    rsi.SellToAddress,
    rsi.SellToCity,
    rsi.SellToState,
    rsi.SellToCountry,
    rsi.SellToPostCode,    
    rsi.BillToAddress,
    rsi.BillToCity,
    rsi.BillToState,
    rsi.BillToCountry,
    rsi.BillToPostCode,
    rsi.InvoiceCancelled,
    rsi.CustomerNumber,
    rsi.InvoiceAmount,
    rsi.InvoiceDiscount,
    rsi.CreatedOn,
    rsi.CreatedOnDate,
    CASE WHEN rsi.[LastUpdated] > rsil.LineLastUpdated THEN rsi.[LastUpdated] ELSE rsil.LineLastUpdated END AS LastUpdated,
    CASE WHEN rsi.[LastUpdatedDate] > rsil.LineLastUpdatedDate THEN rsi.[LastUpdatedDate] ELSE rsil.LineLastUpdatedDate END AS LastUpdatedDate,
    rsil.InvoiceLineID,
    rsil.ItemNumber,
    rsil.LineQuantity,
    rsil.LineAmount,
    rsil.LineType,
    rsil.LineGenProdPostingGroup,
    rsil.LinePostingGroup,
    rsil.LineCreatedOn,
    rsil.LineCreatedOnDate,
    rsil.LineLastUpdated,
    rsil.LineLastUpdatedDate
FROM RankedSalesInvoice rsi
LEFT JOIN RankedSalesInvoiceLine rsil ON rsi.InvoiceNumber = rsil.InvoiceNumber
WHERE rsi.rn = 1 AND rsil.rn = 1
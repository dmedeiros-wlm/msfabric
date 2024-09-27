CREATE   VIEW [Silver].[vQuote]
AS
WITH RankedQuotes AS (
    SELECT 
        Id AS QuoteID,
        UPPER(LTRIM(RTRIM(quotenumber))) as QuoteNumber,
        wsi_account AS AccountID,
        opportunityid AS OpportunityID,
        totalamount AS TotalAmount,
        effectivefrom AS EffectiveFrom,
        effectiveto AS EffectiveTo,      
        createdon AS CreatedOn,
        CAST(createdon AS DATE) AS CreatedOnDate,
        createdby AS CreatedByUserID,
        modifiedon AS LastUpdated,
        CAST(modifiedon AS DATE) AS LastUpdatedDate,
        modifiedby AS LastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Quote]
),
RankedQuoteDetails AS (
    SELECT 
        Id AS QuoteLineID,
        quoteid AS QuoteID,
        UPPER(LTRIM(RTRIM(productnumber))) as ProductNumber,      
        createdon AS LineCreatedOn,
        CAST(createdon AS DATE) AS LineCreatedOnDate,
        createdby AS LineCreatedByUserID,
        modifiedon AS LineLastUpdated,
        CAST(modifiedon AS DATE) AS LineLastUpdatedDate,
        modifiedby AS LineLastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[QuoteDetail]
)
SELECT
    q.QuoteID,
    q.QuoteNumber,
    q.AccountID,
    q.OpportunityID,
    q.TotalAmount,
    q.EffectiveFrom,
    q.EffectiveTo,       
    q.CreatedOn,
    q.CreatedOnDate,
    q.CreatedByUserID,
    CASE WHEN q.[LastUpdated] > qd.LineLastUpdated THEN q.[LastUpdated] ELSE qd.LineLastUpdated END AS LastUpdated,
    CASE WHEN q.[LastUpdatedDate] > qd.LineLastUpdatedDate THEN q.[LastUpdatedDate] ELSE qd.LineLastUpdatedDate END AS LastUpdatedDate,
    q.LastUpdatedByUserID,
    qd.QuoteLineID,
    qd.ProductNumber,
    qd.LineCreatedOn,
    qd.LineCreatedOnDate,
    qd.LineCreatedByUserID,
    qd.LineLastUpdated,
    qd.LineLastUpdatedDate,
    qd.LineLastUpdatedByUserID
FROM RankedQuotes q
LEFT JOIN RankedQuoteDetails qd ON q.QuoteID = qd.QuoteID
WHERE q.rn = 1 AND qd.rn = 1
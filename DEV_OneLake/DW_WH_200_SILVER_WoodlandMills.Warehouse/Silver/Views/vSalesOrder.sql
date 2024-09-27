CREATE   VIEW [Silver].[vSalesOrder]
AS
WITH RankedSalesOrders AS (
    SELECT 
        so.Id AS SalesOrderID,
        UPPER(LTRIM(RTRIM(so.ordernumber))) AS OrderNumber,
        UPPER(LTRIM(RTRIM(so.name))) AS OrderName,
        UPPER(LTRIM(RTRIM(so.wsi_bcsalesorderno))) AS BcOrderNumber,
        so.submitdate AS SubmitDate,
        so.totalamount AS TotalAmount,
        so.wsi_totalweight AS TotalWeight,
        so.wsi_datesentto3pl AS DateSentTo3pl,
        so.wsi_account AS AccountID,
        so.opportunityid AS OpportunityID,
        so.quoteid AS QuoteID,
        COALESCE(oom.LocalizedLabel, '') AS OriginOfOrder,
        COALESCE(sem.LocalizedLabel, '') AS Status,
        COALESCE(stm.LocalizedLabel, '') AS StatusReason,    
        so.createdon AS CreatedOn,
        CAST(so.createdon AS DATE) AS CreatedOnDate,
        so.createdby AS CreatedByUserID,
        so.modifiedon AS LastUpdated,
        CAST(so.modifiedon AS DATE) AS LastUpdatedDate,
        so.modifiedby AS LastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY so.Id ORDER BY so.modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SalesOrder] so
    LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[OptionsetMetadata] oom 
        ON oom.EntityName = 'salesorder' AND oom.OptionSetName = 'wsi_originoforder' AND oom.[Option] = so.wsi_originoforder
    LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[StatusMetadata] stm 
        ON stm.EntityName = 'salesorder' AND stm.[Status] = so.statuscode
    LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[StateMetadata] sem 
        ON sem.EntityName = 'salesorder' AND sem.[State] = so.statecode
),
RankedSalesOrderDetails AS (
    SELECT 
        Id AS SalesOrderLineID, 
        salesorderid AS SalesOrderID,
        UPPER(LTRIM(RTRIM(productnumber))) as ProductNumber,       
        createdon AS LineCreatedOn,
        CAST(createdon AS DATE) AS LineCreatedOnDate,
        createdby AS LineCreatedByUserID,
        modifiedon AS LineLastUpdated,
        CAST(modifiedon AS DATE) AS LineLastUpdatedDate,
        modifiedby AS LineLastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SalesOrderDetail]
)
SELECT
    rso.SalesOrderID,
    rso.OrderNumber,
    rso.OrderName,
    rso.BcOrderNumber,
    rso.SubmitDate,
    rso.TotalAmount,
    rso.TotalWeight,
    rso.DateSentTo3pl,
    rso.AccountID,
    rso.OpportunityID,
    rso.QuoteID,
    rso.OriginOfOrder,
    rso.Status,
    rso.StatusReason,    
    rso.CreatedOn,
    rso.CreatedOnDate,
    rso.CreatedByUserID,
    CASE WHEN rso.[LastUpdated] > rsod.LineLastUpdated THEN rso.[LastUpdated] ELSE rsod.LineLastUpdated END AS LastUpdated,
    CASE WHEN rso.[LastUpdatedDate] > rsod.LineLastUpdatedDate THEN rso.[LastUpdatedDate] ELSE rsod.LineLastUpdatedDate END AS LastUpdatedDate,
    rso.LastUpdatedByUserID,
    rsod.SalesOrderLineID, 
    rsod.ProductNumber,       
    rsod.LineCreatedOn,
    rsod.LineCreatedOnDate,
    rsod.LineCreatedByUserID,
    rsod.LineLastUpdated,
    rsod.LineLastUpdatedDate,
    rsod.LineLastUpdatedByUserID
FROM RankedSalesOrders rso
LEFT JOIN RankedSalesOrderDetails rsod ON rso.SalesOrderID = rsod.SalesOrderID
WHERE rso.rn = 1 AND rsod.rn = 1
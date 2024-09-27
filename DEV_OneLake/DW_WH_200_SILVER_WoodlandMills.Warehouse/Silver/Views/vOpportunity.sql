CREATE   VIEW [Silver].[vOpportunity]
AS
WITH RankedOpportunities AS (
    SELECT 
        Id AS OpportunityID, 
        UPPER(LTRIM(RTRIM(name))) AS OpportunityName,
        originatingleadid AS LeadID,
        parentaccountid AS AccountID,       
        createdon AS CreatedOn,
        CAST(createdon AS DATE) AS CreatedOnDate,
        createdby AS CreatedByUserID,
        modifiedon AS LastUpdated,
        CAST(modifiedon AS DATE) AS LastUpdatedDate,
        modifiedby AS LastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Opportunity]
)
SELECT
    OpportunityID, 
    OpportunityName,
    LeadID,
    AccountID,
    CreatedOn,
    CreatedOnDate,
    CreatedByUserID,
    LastUpdated,
    LastUpdatedDate,
    LastUpdatedByUserID
FROM RankedOpportunities
WHERE rn = 1
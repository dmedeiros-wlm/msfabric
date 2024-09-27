CREATE   VIEW [Silver].[vAccount]
AS
WITH RankedAccounts AS (
    SELECT 
        Id AS AccountID,
        originatingleadid AS LeadID,     
        createdon AS CreatedOn,
        CAST(createdon AS DATE) AS CreatedOnDate,
        createdby AS CreatedByUserID,
        modifiedon AS LastUpdated,
        CAST(modifiedon AS DATE) AS LastUpdatedDate,
        modifiedby AS LastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Account]
)
SELECT
    AccountID,
    LeadID,       
    CreatedOn,
    CreatedOnDate,
    CreatedByUserID,
    LastUpdated,
    LastUpdatedDate,
    LastUpdatedByUserID
FROM RankedAccounts
WHERE rn = 1
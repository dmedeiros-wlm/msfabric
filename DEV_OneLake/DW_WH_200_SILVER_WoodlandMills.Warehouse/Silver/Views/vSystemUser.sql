CREATE   VIEW [Silver].[vSystemUser]
AS
WITH RankedSystemUsers AS (
    SELECT 
        Id AS UserID,
        UPPER(LTRIM(RTRIM(fullname))) AS FullName,        
        createdon AS CreatedOn,
        CAST(createdon AS DATE) AS CreatedOnDate,
        createdby AS CreatedByUserID,
        modifiedon AS LastUpdated,
        CAST(modifiedon AS DATE) AS LastUpdatedDate,
        modifiedby AS LastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser]
)
SELECT
    UserID,
    FullName,        
    CreatedOn,
    CreatedOnDate,
    CreatedByUserID,
    LastUpdated,
    LastUpdatedDate,
    LastUpdatedByUserID
FROM RankedSystemUsers
WHERE rn = 1
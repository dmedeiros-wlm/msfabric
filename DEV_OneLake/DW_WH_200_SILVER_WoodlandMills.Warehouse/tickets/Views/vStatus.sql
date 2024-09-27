CREATE   VIEW [tickets].[vStatus]
AS
SELECT DISTINCT
    sem.[State] AS StatusID,
    COALESCE(sem.LocalizedLabel, '') AS Status
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[StateMetadata] sem
WHERE sem.EntityName = 'incident';
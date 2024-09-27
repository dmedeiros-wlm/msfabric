CREATE   VIEW [tickets].[vEditor]
AS
SELECT
    su.Id AS EditorID,
    UPPER(LTRIM(RTRIM(su.fullname))) AS EditorFullName,
    su.createdon AS EditorCreatedOn,
    CAST(su.createdon AS DATE) AS EditorCreatedOnDate,
    UPPER(LTRIM(RTRIM(cb.fullname))) AS EditorCreatedByFullName,
    su.modifiedon AS EditorLastUpdated,
    CAST(su.modifiedon AS DATE) AS EditorLastUpdatedDate,
    UPPER(LTRIM(RTRIM(lub.fullname))) AS EditorLastUpdatedByFullName
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] su
LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] cb ON su.createdby = cb.Id
LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] lub ON su.modifiedby = lub.Id
WHERE su.Id IN (SELECT DISTINCT modifiedby FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Incident]);
CREATE   VIEW [tickets].[vResolutionRep]
AS
SELECT
    su.Id AS RepID,
    UPPER(LTRIM(RTRIM(su.fullname))) AS RepFullName,
    su.createdon AS RepCreatedOn,
    CAST(su.createdon AS DATE) AS RepCreatedOnDate,
    UPPER(LTRIM(RTRIM(cb.fullname))) AS RepCreatedByFullName,
    su.modifiedon AS RepLastUpdated,
    CAST(su.modifiedon AS DATE) AS RepLastUpdatedDate,
    UPPER(LTRIM(RTRIM(lub.fullname))) AS RepLastUpdatedByFullName
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] su
LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] cb ON su.createdby = cb.Id
LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] lub ON su.modifiedby = lub.Id
WHERE su.Id IN (SELECT DISTINCT createdby FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[IncidentResolution]);
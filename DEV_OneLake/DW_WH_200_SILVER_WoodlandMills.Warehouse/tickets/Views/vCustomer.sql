CREATE   VIEW [tickets].[vCustomer]
AS
SELECT
    acc.Id AS CustomerID,
    acc.createdon AS CustomerCreatedOn,
    CAST(acc.createdon AS DATE) AS CustomerCreatedOnDate,
    UPPER(LTRIM(RTRIM(cb.fullname))) AS CustomerCreatedByFullName,
    acc.modifiedon AS CustomerLastUpdated,
    CAST(acc.modifiedon AS DATE) AS CustomerLastUpdatedDate,
    UPPER(LTRIM(RTRIM(lub.fullname))) AS CustomerLastUpdatedByFullName
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Account] acc
LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] cb ON acc.createdby = cb.Id
LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] lub ON acc.modifiedby = lub.Id
WHERE acc.Id IN (SELECT DISTINCT customerid FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Incident]);
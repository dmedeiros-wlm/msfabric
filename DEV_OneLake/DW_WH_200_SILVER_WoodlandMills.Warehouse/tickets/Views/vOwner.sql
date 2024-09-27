CREATE   VIEW [tickets].[vOwner]
AS
WITH TechGroups AS (
    SELECT
        'TechGroup1' AS GroupName,
        RepID
    FROM (
        VALUES
        -- List of RepIDs for TechGroup1
        ('E1523D38-6F7D-EC11-8D21-000D3AF4DA5F'), ('DB9C353E-6F7D-EC11-8D21-000D3AF4DA5F')
    ) AS TG1(RepID)
    UNION ALL
    SELECT
        'TechGroup2' AS GroupName,
        RepID
    FROM (
        VALUES
        -- List of RepIDs for TechGroup2
        ('C435801E-6689-EC11-93B0-0022486D6541')
    ) AS TG2(RepID)
)
SELECT
    su.Id AS OwnerID,
    UPPER(LTRIM(RTRIM(su.fullname))) AS OwnerFullName,
    su.createdon AS OwnerCreatedOn,
    CAST(su.createdon AS DATE) AS OwnerCreatedOnDate,
    UPPER(LTRIM(RTRIM(cb.fullname))) AS OwnerCreatedByFullName,
    su.modifiedon AS OwnerLastUpdated,
    CAST(su.modifiedon AS DATE) AS OwnerLastUpdatedDate,
    UPPER(LTRIM(RTRIM(lub.fullname))) AS OwnerLastUpdatedByFullName,
    CASE
        WHEN UPPER(LTRIM(RTRIM(su.Id))) IN (SELECT RepID FROM TechGroups WHERE GroupName = 'TechGroup1') THEN 'Yes'
        ELSE 'No'
    END AS TechGroup1,
    CASE
        WHEN UPPER(LTRIM(RTRIM(su.Id))) IN (SELECT RepID FROM TechGroups WHERE GroupName = 'TechGroup2') THEN 'Yes'
        ELSE 'No'
    END AS TechGroup2
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] su
LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] cb ON su.createdby = cb.Id
LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SystemUser] lub ON su.modifiedby = lub.Id
WHERE su.Id IN (SELECT DISTINCT ownerid FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Incident]);
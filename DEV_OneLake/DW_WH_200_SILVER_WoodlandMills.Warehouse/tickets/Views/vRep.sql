CREATE   VIEW [tickets].[vRep]
AS
WITH TechGroups AS (
    SELECT
        'TechGroup1' AS GroupName,
        RepID
    FROM (
        VALUES
        -- List of OwnerIDs for TechGroup1
        ('30223E8F-F7BA-EE11-9079-002248B0AB65'), ('37DADBF3-2EDA-EE11-904D-002248B0A984'), ('CFDFC561-E517-ED11-B83E-000D3AF4FA76'), ('C435801E-6689-EC11-93B0-0022486D6541')
    ) AS TG1(RepID)
    UNION ALL
    SELECT
        'TechGroup2' AS GroupName,
        RepID
    FROM (
        VALUES
        -- List of OwnerIDs for TechGroup2
        ('30223E8F-F7BA-EE11-9079-002248B0AB65'), ('37DADBF3-2EDA-EE11-904D-002248B0A984'), ('CFDFC561-E517-ED11-B83E-000D3AF4FA76'), ('E1523D38-6F7D-EC11-8D21-000D3AF4DA5F'), ('DB9C353E-6F7D-EC11-8D21-000D3AF4DA5F')
    ) AS TG2(RepID)
)
SELECT
    su.Id AS RepID,
    UPPER(LTRIM(RTRIM(su.fullname))) AS RepFullName,
    su.createdon AS RepCreatedOn,
    CAST(su.createdon AS DATE) AS RepCreatedOnDate,
    UPPER(LTRIM(RTRIM(cb.fullname))) AS RepCreatedByFullName,
    su.modifiedon AS RepLastUpdated,
    CAST(su.modifiedon AS DATE) AS RepLastUpdatedDate,
    UPPER(LTRIM(RTRIM(lub.fullname))) AS RepLastUpdatedByFullName,
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
WHERE su.Id IN (SELECT DISTINCT createdby FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Incident]);
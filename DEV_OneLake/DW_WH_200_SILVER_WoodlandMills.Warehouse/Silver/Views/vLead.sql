CREATE   VIEW [Silver].[vLead]
AS
WITH RankedLeads AS (
    SELECT 
        l.Id AS LeadID,
        UPPER(LTRIM(RTRIM(l.address1_country))) AS Country,
        COALESCE(lsm.LocalizedLabel, '') AS LeadSource,
        l.parentaccountid AS AccountID,       
        l.createdon AS CreatedOn,
        CAST(l.createdon AS DATE) AS CreatedOnDate,
        l.createdby AS CreatedByUserID,
        l.modifiedon AS LastUpdated,
        CAST(l.modifiedon AS DATE) AS LastUpdatedDate,
        l.modifiedby AS LastUpdatedByUserID,
        ROW_NUMBER() OVER (PARTITION BY l.Id ORDER BY l.modifiedon DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Lead] l
    LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[OptionsetMetadata] lsm 
        ON lsm.EntityName = 'lead' AND lsm.OptionSetName = 'leadsourcecode' AND lsm.[Option] = l.leadsourcecode
),
MappedReasons AS (
    SELECT
        l.Id,
        STRING_AGG(COALESCE(rfm.LocalizedLabel, ''), ';') WITHIN GROUP (ORDER BY value) AS ReasonForCall
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Lead] l
    CROSS APPLY STRING_SPLIT(l.wsi_reasonforcall, ';') s
    LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[GlobalOptionsetMetadata] rfm
        ON rfm.EntityName = 'lead' AND rfm.OptionSetName = 'wsi_reasonforcall' AND rfm.[Option] = s.value
    GROUP BY l.Id
),
MappedProducts AS (
    SELECT
        l.Id,
        STRING_AGG(COALESCE(pim.LocalizedLabel, ''), ';') WITHIN GROUP (ORDER BY value) AS ProductsInquiringAbout
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Lead] l
    CROSS APPLY STRING_SPLIT(l.wsi_productsinquiringabout, ';') s
    LEFT JOIN [DE_LH_100_BRONZE_WoodlandMills].[dbo].[GlobalOptionsetMetadata] pim
        ON pim.EntityName = 'lead' AND pim.OptionSetName = 'wsi_productsinquiringabout' AND pim.[Option] = s.value
    GROUP BY l.Id
)
SELECT
    rl.LeadID,
    rl.Country,
    rl.LeadSource,
    mr.ReasonForCall,
    mp.ProductsInquiringAbout,
    rl.AccountID,       
    rl.CreatedOn,
    rl.CreatedOnDate,
    rl.CreatedByUserID,
    rl.LastUpdated,
    rl.LastUpdatedDate,
    rl.LastUpdatedByUserID
FROM RankedLeads rl
LEFT JOIN MappedProducts mp ON rl.LeadID = mp.Id
LEFT JOIN MappedReasons mr ON rl.LeadID = mr.Id
WHERE rl.rn = 1
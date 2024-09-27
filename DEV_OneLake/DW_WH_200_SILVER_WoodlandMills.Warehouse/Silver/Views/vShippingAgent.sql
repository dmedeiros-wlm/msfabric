CREATE   VIEW [Silver].[vShippingAgent]
AS
WITH RankedShippingAgent AS (
    SELECT 
        systemId AS ShippingAgentID,
        UPPER(LTRIM(RTRIM(code))) AS ShippingAgentCode,
        UPPER(LTRIM(RTRIM(name))) AS ShippingAgentName,
        -- systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[ShippingAgent]
), RankedShippingAgentCode AS (
    SELECT 
        systemId AS ShippingAgentCodeID,
        UPPER(LTRIM(RTRIM(ediShipper))) AS EdiShipper,
        UPPER(LTRIM(RTRIM(shippingAgent))) AS ShippingAgentCode,
        UPPER(LTRIM(RTRIM(tp))) AS TradingPartner,
        systemCreatedBy AS CodeCreatedByUserID,
        systemCreatedAt AS CodeCreatedOn,
        CAST(systemCreatedAt AS DATE) AS CodeCreatedOnDate,
        systemModifiedBy AS CodeLastUpdatedByUserID,
        systemModifiedAt AS CodeLastUpdated,
        CAST(systemModifiedAt AS DATE) AS CodeLastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[ShippingAgentCode]
)
SELECT
    rsa.ShippingAgentID,
    rsa.ShippingAgentCode,
    rsa.ShippingAgentName,
    -- rsa.CreatedByUserID,
    rsa.CreatedOn,
    rsa.CreatedOnDate,
    rsa.LastUpdatedByUserID,
    CASE WHEN rsa.[LastUpdated] > rsac.CodeLastUpdated THEN rsa.[LastUpdated] ELSE rsac.CodeLastUpdated END AS LastUpdated,
    CASE WHEN rsa.[LastUpdatedDate] > rsac.CodeLastUpdatedDate THEN rsa.[LastUpdatedDate] ELSE rsac.CodeLastUpdatedDate END AS LastUpdatedDate,
    rsac.ShippingAgentCodeID,
    rsac.EdiShipper,
    rsac.TradingPartner,
    rsac.CodeCreatedByUserID,
    rsac.CodeCreatedOn,
    rsac.CodeCreatedOnDate,
    rsac.CodeLastUpdatedByUserID,
    rsac.CodeLastUpdated,
    rsac.CodeLastUpdatedDate
FROM RankedShippingAgent rsa
LEFT JOIN RankedShippingAgentCode rsac ON rsa.ShippingAgentCode = rsac.ShippingAgentCode
WHERE rsa.rn = 1 AND rsac.rn = 1
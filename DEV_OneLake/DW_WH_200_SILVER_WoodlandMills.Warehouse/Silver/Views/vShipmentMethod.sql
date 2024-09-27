CREATE   VIEW [Silver].[vShipmentMethod]
AS
WITH RankedShipmentMethod AS (
    SELECT 
        systemId AS ShipmentMethodID,
        UPPER(LTRIM(RTRIM(code))) AS ShipmentMethodCode,
        UPPER(LTRIM(RTRIM(description))) AS ShipmentMethodDescription,
        -- systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[ShipmentMethod]
)
SELECT
    rsm.ShipmentMethodID,
    rsm.ShipmentMethodCode,
    rsm.ShipmentMethodDescription,
    -- rsm.CreatedByUserID,
    rsm.CreatedOn,
    rsm.CreatedOnDate,
    rsm.LastUpdatedByUserID,
    rsm.LastUpdated,
    rsm.LastUpdatedDate
FROM RankedShipmentMethod rsm
WHERE rsm.rn = 1
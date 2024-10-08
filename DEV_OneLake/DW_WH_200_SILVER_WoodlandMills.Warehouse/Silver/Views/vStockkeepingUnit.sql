CREATE   VIEW [Silver].[vStockkeepingUnit]
AS
WITH RankedStockkeepingUnit AS (
    SELECT 
        systemId AS StockkeepingUnitID,        
        UPPER(LTRIM(RTRIM(itemNo))) AS ItemNumber,
        UPPER(LTRIM(RTRIM(locationCode))) AS ShippingLocation,
        UPPER(LTRIM(RTRIM(replenishmentSystem))) AS ReplenishmentSystem,
        UPPER(LTRIM(RTRIM(leadTimeCalculation))) AS LeadTimeCalculation,
        UPPER(LTRIM(RTRIM(vendorNo))) AS VendorNumber,
        UPPER(LTRIM(RTRIM(reorderingPolicy))) AS ReorderingPolicy,
        UPPER(LTRIM(RTRIM(safetyLeadTime))) AS SafetyLeadTime,
        qtyOnPurchOrder AS QuantityOnPurchaseOrder,
        qtyInTransit AS QuantityInTransity,
        qtyOnSalesOrder AS QuantityOnSalesOrder,
        inventory AS Inventory,
        unitCost AS UnitCost,
        lastDirectCost AS LastDirectCost,
        safetyStockQuantity AS SafetyStockQuantity,
        reorderPoint AS ReorderPoint,
        reorderQuantity AS ReorderQuantity,
        maximumInventory AS MaximumInventory,
        minimumOrderQuantity AS MinimumOrderQuantity,
        maximumOrderQuantity AS MaximumOrderQuantity,
        orderMultiple AS OrderMultiple,
        systemCreatedBy AS CreatedByUserID,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedBy AS LastUpdatedByUserID,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[StockkeepingUnit]
)
SELECT
    rsu.StockkeepingUnitID,
    rsu.ItemNumber,
    rsu.ShippingLocation,
    rsu.ReplenishmentSystem,
    rsu.LeadTimeCalculation,
    rsu.VendorNumber,
    rsu.ReorderingPolicy,
    rsu.SafetyLeadTime,
    rsu.QuantityOnPurchaseOrder,
    rsu.QuantityInTransity,
    rsu.QuantityOnSalesOrder,
    rsu.Inventory,
    rsu.UnitCost,
    rsu.LastDirectCost,
    rsu.SafetyStockQuantity,
    rsu.ReorderPoint,
    rsu.ReorderQuantity,
    rsu.MaximumInventory,
    rsu.MinimumOrderQuantity,
    rsu.MaximumOrderQuantity,
    rsu.OrderMultiple,
    rsu.CreatedByUserID,
    rsu.CreatedOn,
    rsu.CreatedOnDate,
    rsu.LastUpdatedByUserID,
    rsu.LastUpdated,
    rsu.LastUpdatedDate
FROM RankedStockkeepingUnit rsu
WHERE rsu.rn = 1
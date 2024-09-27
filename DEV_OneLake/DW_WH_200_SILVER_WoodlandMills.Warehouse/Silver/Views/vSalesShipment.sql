CREATE   VIEW [Silver].[vSalesShipment]
AS
WITH RankedSalesShipment AS (
    SELECT 
        systemId AS ShipmentID,
        UPPER(LTRIM(RTRIM([no]))) AS ShipmentNumber,
        UPPER(LTRIM(RTRIM(orderNo))) AS OrderNumber,
        postingDate AS ShipmentPostingDate,
        UPPER(LTRIM(RTRIM(selltoCustomerNo))) AS CustomerNumber,
        UPPER(LTRIM(RTRIM(shiptoAddress))) AS ShipToAddress,
        UPPER(LTRIM(RTRIM(shiptoCity))) AS ShipToCity,
        UPPER(LTRIM(RTRIM(shiptoCounty))) AS ShipToState,
        UPPER(LTRIM(RTRIM(shiptoCountryRegionCode))) AS ShipToCountry,
        UPPER(LTRIM(RTRIM(shiptoPostCode))) AS ShipToPostCode,
        UPPER(LTRIM(RTRIM(locationCode))) AS ShippingLocation,
        UPPER(LTRIM(RTRIM(shipmentMethodCode))) AS ShipmentMethod,
        UPPER(LTRIM(RTRIM(shippingAgentCode))) AS ShippingAgent,
        UPPER(LTRIM(RTRIM(shippingAgentServiceCode))) AS ShippingAgentService,
        wsI0032ExpectedShipping AS ExpectedCost,
        wsI0042ExpectedDlvryTime AS ExpectedDeliveryTime,
        wsI0032TotalCubage AS TotalCubage,
        wsI0032TotalWeight AS TotalWeight,
        currencyCode AS Currency,
        currencyFactor AS CurrencyFactor,
        orderDate AS OrderDate,
        documentDate AS ShipmentDocumentDate,
        dueDate AS ShipmentDueDate,
        shipmentDate AS ShipmentDate,
        requestedDeliveryDate AS RequestedDeliveryDate,
        promisedDeliveryDate AS PromisedDeliveryDate,
        systemCreatedAt AS CreatedOn,
        CAST(systemCreatedAt AS DATE) AS CreatedOnDate,
        systemModifiedAt AS LastUpdated,
        CAST(systemModifiedAt AS DATE) AS LastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SalesShipment]
),
RankedSalesShipmentLine AS (
    SELECT 
        systemId AS ShipmentLineID,
        UPPER(LTRIM(RTRIM(documentNo))) AS ShipmentNumber,
        UPPER(LTRIM(RTRIM([no]))) AS ItemNumber,
        quantity AS LineQuantity,
        unitPrice AS LineAmount,
        unitCost AS LineCost,
        unitCostLCY AS LineCostInLocalCurrency,
        wsI0032Cubage AS LineCubage,
        unitVolume AS LineVolume,
        wsI0032Weight AS LineWeight,
        grossWeight AS LineGrossWeight,
        netWeight AS LineNetWeight,
        UPPER(LTRIM(RTRIM(locationCode))) AS LineShippingLocation,
        UPPER(LTRIM(RTRIM(type))) AS LineType,
        UPPER(LTRIM(RTRIM(genProdPostingGroup))) AS LineGenProdPostingGroup,
        UPPER(LTRIM(RTRIM(postingGroup))) AS LinePostingGroup,
        UPPER(LTRIM(RTRIM(itemCategoryCode))) AS ItemCategory,
        postingDate AS LinePostingDate,
        plannedShipmentDate AS LinePlannedShipmentDate,
        shipmentDate AS LineShipmentDate,
        plannedDeliveryDate AS LinePlannedDeliveryDate,
        promisedDeliveryDate AS LinePromisedDeliveryDate,
        systemCreatedAt AS LineCreatedOn,
        CAST(systemCreatedAt AS DATE) AS LineCreatedOnDate,
        systemModifiedAt AS LineLastUpdated,
        CAST(systemModifiedAt AS DATE) AS LineLastUpdatedDate,
        ROW_NUMBER() OVER (PARTITION BY systemId ORDER BY systemModifiedAt DESC) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[SalesShipmentLine]
)
SELECT
    rss.ShipmentID,
    rss.ShipmentNumber,
    rss.OrderNumber,
    rss.ShipmentPostingDate,
    rss.CustomerNumber,
    rss.ShipToAddress,
    rss.ShipToCity,
    rss.ShipToState,
    rss.ShipToCountry,
    rss.ShipToPostCode,
    rss.ShippingLocation,
    rss.ShipmentMethod,
    rss.ShippingAgent,
    rss.ShippingAgentService,
    rss.ExpectedCost,
    rss.ExpectedDeliveryTime,
    rss.TotalCubage,
    rss.TotalWeight,
    rss.Currency,
    rss.CurrencyFactor,
    rss.OrderDate,
    rss.ShipmentDocumentDate,
    rss.ShipmentDueDate,
    rss.ShipmentDate,
    rss.RequestedDeliveryDate,
    rss.PromisedDeliveryDate,
    rss.CreatedOn,
    rss.CreatedOnDate,
    CASE WHEN rss.[LastUpdated] > rssl.LineLastUpdated THEN rss.[LastUpdated] ELSE rssl.LineLastUpdated END AS LastUpdated,
    CASE WHEN rss.[LastUpdatedDate] > rssl.LineLastUpdatedDate THEN rss.[LastUpdatedDate] ELSE rssl.LineLastUpdatedDate END AS LastUpdatedDate,
    rssl.ShipmentLineID,
    rssl.ItemNumber,
    rssl.LineQuantity,
    rssl.LineAmount,
    rssl.LineCost,
    rssl.LineCostInLocalCurrency,
    rssl.LineCubage,
    rssl.LineVolume,
    rssl.LineWeight,
    rssl.LineGrossWeight,
    rssl.LineNetWeight,
    rssl.LineShippingLocation,
    rssl.LineType,
    rssl.LineGenProdPostingGroup,
    rssl.LinePostingGroup,
    rssl.ItemCategory,
    rssl.LinePostingDate,
    rssl.LinePlannedShipmentDate,
    rssl.LineShipmentDate,
    rssl.LinePlannedDeliveryDate,
    rssl.LinePromisedDeliveryDate,
    rssl.LineCreatedOn,
    rssl.LineCreatedOnDate,
    rssl.LineLastUpdated,
    rssl.LineLastUpdatedDate
FROM RankedSalesShipment rss
LEFT JOIN RankedSalesShipmentLine rssl ON rss.ShipmentNumber = rssl.ShipmentNumber
WHERE rss.rn = 1 AND rssl.rn = 1
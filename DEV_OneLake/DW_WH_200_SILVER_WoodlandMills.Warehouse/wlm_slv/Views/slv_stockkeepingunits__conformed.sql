create view "wlm_slv"."slv_stockkeepingunits__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."stockkeepingunits"),

    stockkeepingunits as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as stockkeepingunitid,
            upper(ltrim(rtrim(itemNo))) as itemnumber,
            upper(ltrim(rtrim(description))) as productdescription,
            upper(ltrim(rtrim(locationCode))) as locationcode,
            qtyOnPurchOrder as quantityonpurchaseorder,
            qtyInTransit as quantityintransit,
            qtyOnSalesOrder as quantityonsalesorder,
            inventory,
            unitCost as unitcost,
            lastDirectCost as lastdirectcost,
            replenishmentSystem as replenishmentsystem,
            leadTimeCalculation as leadtimecalculation,
            vendorNo as vendornumber,
            transferFromCode as transferfromlocationcode,
            reorderingPolicy as reorderingpolicy,
            safetyLeadTime as safetyleadtime,
            safetyStockQuantity as safetystockquantity,
            reorderPoint as reorderpoint,
            reorderQuantity as reorderquantity,
            maximumInventory as maximuminventory,
            minimumOrderQuantity as minimumorderquantity,
            maximumOrderQuantity as maximumorderquantity,
            orderMultiple as ordermultiple,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from stockkeepingunits
        where rn = 1
    )

select *
from final;
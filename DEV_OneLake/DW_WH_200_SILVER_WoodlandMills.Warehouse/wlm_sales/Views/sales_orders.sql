create view "wlm_sales"."sales_orders" as with 
    src as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_salesorders_joined"
    ),

    final as (
        select
            concat(salesorderid, '-', salesorderlineid) as CompositeKey,
            salesorderid as SalesOrderID,
            salesorderlineid as SalesOrderLineID,
            currency as CurrencyID,
            originoforder as OriginID,
            status as StatusID,
            state as StateID,
            pricelist as PriceListID,
            totalamount as TotalAmount,
            createdondate as CreatedOnDate,
            lastupdated as LastUpdated
        from src
        where createdondate > '2023-01-01'
    )
select *
from final;
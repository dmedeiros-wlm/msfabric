create view "wlm_sales"."sales_pricelist" as with 
    src as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__pricelists"
    ),

    final as (
        select pricelistid as PriceListID, pricelist as PriceList
        from src
    )
select *
from final;
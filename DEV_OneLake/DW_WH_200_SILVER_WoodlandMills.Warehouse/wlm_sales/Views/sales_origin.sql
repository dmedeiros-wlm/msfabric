create view "wlm_sales"."sales_origin" as with 
    src as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_salesorders_options"
    ),

    final as (
        select numericallabel as OriginID, label as Origin, case when label = 'Website' then 'Website' else 'Manual' end as OriginCategory
        from src
        where columnname = 'wsi_originoforder'
    )
select *
from final;
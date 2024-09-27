create view "wlm_sales"."sales_status" as with 
    src as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_salesorders_statuses"
    ),

    final as (
        select numericallabel as StatusID, label as Status
        from src
    )
select *
from final;
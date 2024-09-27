create view "wlm_sales"."sales_state" as with 
    src as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_salesorders_states"
    ),

    final as (
        select numericallabel as StateID, label as State
        from src
    )
select *
from final;
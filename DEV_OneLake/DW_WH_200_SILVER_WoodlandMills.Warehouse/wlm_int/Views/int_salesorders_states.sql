create view "wlm_int"."int_salesorders_states" as with
    src as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__states"
    ),

    final as (
        select *
        from src
        where tablename = 'salesorder'
    )
select *
from final;
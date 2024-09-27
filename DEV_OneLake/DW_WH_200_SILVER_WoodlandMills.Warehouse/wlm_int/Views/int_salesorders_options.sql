create view "wlm_int"."int_salesorders_options" as with
    src as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__options"
    ),

    final as (
        select *
        from src
        where tablename = 'salesorder'
    )
select *
from final;
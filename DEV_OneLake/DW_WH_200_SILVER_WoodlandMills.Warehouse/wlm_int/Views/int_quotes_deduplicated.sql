create view "wlm_int"."int_quotes_deduplicated" as with
    quotes as (
        select
            *, row_number() over (partition by quoteid order by lastupdated desc) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__quotes"
    ),

    final as (select * from quotes where rn = 1)

select *
from final;
create view "wlm_int"."int_leads_deduplicated" as with
    leads as (
        select
            *, row_number() over (partition by leadid order by lastupdated desc) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__leads"
    ),

    final as (
        select
            *,
            case
                when reasonforcall = '866490003'
                then 'Catalogue Request'
                else 'General'
            end as leadtype
        from leads
        where rn = 1
    )

select *
from final;
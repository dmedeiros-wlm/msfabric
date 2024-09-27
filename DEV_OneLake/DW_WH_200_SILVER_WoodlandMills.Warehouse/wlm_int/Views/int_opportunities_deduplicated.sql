create view "wlm_int"."int_opportunities_deduplicated" as with
    opportunities as (
        select
            *,
            row_number() over (
                partition by opportunityid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__opportunities"
    ),

    final as (
        select
            *,
            case
                when productsinquiringabout = '930820000'
                then 'Catalogue Request'
                else 'General'
            end as opportunitytype
        from opportunities
        where rn = 1
    )

select *
from final;
create view "wlm_int"."int_catalogueopportunities_deduplicated" as with
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
            opportunityid as eventid,
            'Catalogue Request - Opportunity' as eventtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated
        from opportunities
        where rn = 1 and productsinquiringabout = '930820000'
    )

select *
from final;
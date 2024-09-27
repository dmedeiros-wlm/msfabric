create view "wlm_int"."int_catalogueleads_deduplicated" as with
    leads as (
        select
            *,
            row_number() over (
                partition by leadid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__leads"
    ),

    final as (
        select
            leadid as eventid,
            'Catalogue Request - Lead' as eventtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated
        from leads
        where rn = 1 and reasonforcall = '866490003'
    )

select *
from final;
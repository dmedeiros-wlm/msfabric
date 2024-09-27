create view "wlm_int"."int_chats_deduplicated" as with
    chats as (
        select
            *, row_number() over (partition by chatid order by lastupdated desc) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__omnichannelliveworkitems"
    ),

    final as (
        select
            *,
            case
                when queueid = 'e0baf14a-be37-ee11-bdf4-000d3a09d4ee'
                then 'Sales Queue'
                when queueid = 'f40757f1-bf37-ee11-bdf4-000d3a09d4ee'
                then 'Tech Queue'
                else 'General'
            end as chattype
        from chats
        where rn = 1
    )

select *
from final;
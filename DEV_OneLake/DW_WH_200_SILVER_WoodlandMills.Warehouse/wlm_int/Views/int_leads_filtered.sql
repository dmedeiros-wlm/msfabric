create view "wlm_int"."int_leads_filtered" as with
    leads as (
        select
            *,
            row_number() over (
                partition by leadid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__leads"
    ),

    team_positions as (
        select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"
    ),

    final as (
        select
            l.*,
            tp.teamposition as creatorposition
        from leads as l
        left join team_positions as tp on l.createdbyuserid = tp.userid
        where l.rn = 1 and tp.teamposition is not null
    )

select *
from final;
create view "wlm_int"."int_opportunities_filtered" as with
    opportunities as (
        select
            *,
            row_number() over (
                partition by opportunityid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__opportunities"
    ),

    team_positions as (
        select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"
    ),

    final as (
        select
            o.*,
            tp.teamposition as creatorposition
        from opportunities as o
        left join team_positions as tp on o.createdbyuserid = tp.userid
        where o.rn = 1 and tp.teamposition is not null
    )

select *
from final;
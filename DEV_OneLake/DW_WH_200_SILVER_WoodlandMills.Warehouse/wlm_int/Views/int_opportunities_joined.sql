create view "wlm_int"."int_opportunities_joined" as with
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
            coalesce(op.teamposition, 'Others') as ownerposition,
            coalesce(cp.teamposition, 'Others') as creatorposition
        from opportunities as o
        left join team_positions as op on o.ownerid = op.userid
        left join team_positions as cp on o.createdbyuserid = cp.userid
        where o.rn = 1
    )

select *
from final;
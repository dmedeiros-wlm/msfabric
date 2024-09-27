create view "wlm_int"."int_leads_joined" as with
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
            coalesce(op.teamposition, 'Others') as ownerposition,
            coalesce(cp.teamposition, 'Others') as creatorposition
        from leads as l
        left join team_positions as op on l.ownerid = op.userid
        left join team_positions as cp on l.createdbyuserid = cp.userid

        where l.rn = 1
    )

select *
from final;
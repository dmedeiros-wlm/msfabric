create view "wlm_int"."int_omnichannel_joined" as with
    chats as (
        select
            *,
            row_number() over (
                partition by ocliveworkitemid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__omnichannelliveworkitems"
    ),

    team_positions as (
        select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"
    ),

    final as (
        select
            c.*,
            coalesce(op.teamposition, 'Others') as ownerposition,
            coalesce(cp.teamposition, 'Others') as creatorposition,
            coalesce(ap.teamposition, 'Others') as activeagentposition
        from chats as c
        left join team_positions as op on c.ownerid = op.userid
        left join team_positions as cp on c.createdbyuserid = cp.userid
        left join team_positions as ap on c.activeagentid = ap.userid
        where c.rn = 1
    )

select *
from final;
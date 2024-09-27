create view "wlm_int"."int_omnichannel_filtered" as with
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
            tp.teamposition as ownerposition
        from chats as c
        left join team_positions as tp on c.ownerid = tp.userid
        where c.rn = 1 and tp.teamposition is not null
    )

select *
from final;
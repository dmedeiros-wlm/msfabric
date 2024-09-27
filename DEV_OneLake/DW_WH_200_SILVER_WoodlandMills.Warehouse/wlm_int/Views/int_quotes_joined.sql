create view "wlm_int"."int_quotes_joined" as with
    quotes as (
        select
            *,
            row_number() over (
                partition by quoteid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__quotes"
    ),

    team_positions as (
        select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"
    ),

    final as (
        select
            q.*,
            coalesce(op.teamposition, 'Others') as ownerposition,
            coalesce(cp.teamposition, 'Others') as creatorposition
        from quotes as q
        left join team_positions as op on q.ownerid = op.userid
        left join team_positions as cp on q.createdbyuserid = cp.userid
        where q.rn = 1
    )

select *
from final;
create view "wlm_int"."int_phonecalls_joined" as with
    phonecalls as (
        select
            *,
            row_number() over (
                partition by phonecallid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__phonecalls"
    ),

    team_positions as (
        select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"
    ),

    final as (
        select
            pc.*,
            coalesce(op.teamposition, 'Others') as ownerposition,
            coalesce(cp.teamposition, 'Others') as creatorposition
        from phonecalls as pc
        left join team_positions as op on pc.ownerid = op.userid
        left join team_positions as cp on pc.createdbyuserid = cp.userid
        where pc.rn = 1
    )

select *
from final;
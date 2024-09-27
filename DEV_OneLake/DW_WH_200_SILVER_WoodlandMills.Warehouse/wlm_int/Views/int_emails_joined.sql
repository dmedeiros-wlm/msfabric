create view "wlm_int"."int_emails_joined" as with
    emails as (
        select
            *,
            row_number() over (
                partition by emailid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__emails"
    ),

    team_positions as (
        select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"
    ),

    final as (
        select
            e.*,
            coalesce(op.teamposition, 'Others') as ownerposition,
            coalesce(cp.teamposition, 'Others') as creatorposition
        from emails as e
        left join team_positions as op on e.ownerid = op.userid
        left join team_positions as cp on e.createdbyuserid = cp.userid
        where e.rn = 1
    )

select *
from final;
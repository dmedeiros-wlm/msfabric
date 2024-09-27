create view "wlm_int"."int_emails_filtered" as with
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
            tp.teamposition as creatorposition
        from emails as e
        left join team_positions as tp on e.createdbyuserid = tp.userid
        where e.rn = 1 and tp.teamposition is not null
    )

select *
from final;
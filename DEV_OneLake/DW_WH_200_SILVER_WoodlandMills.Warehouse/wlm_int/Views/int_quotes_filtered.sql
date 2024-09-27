create view "wlm_int"."int_quotes_filtered" as with
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
            tp.teamposition as creatorposition
        from quotes as q
        left join team_positions as tp on q.createdbyuserid = tp.userid
        where q.rn = 1 and tp.teamposition is not null
    )

select *
from final;
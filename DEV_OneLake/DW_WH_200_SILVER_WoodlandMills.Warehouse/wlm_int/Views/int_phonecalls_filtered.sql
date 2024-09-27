create view "wlm_int"."int_phonecalls_filtered" as with
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
            tp.teamposition as creatorposition
        from phonecalls as pc
        left join team_positions as tp on pc.createdbyuserid = tp.userid
        where pc.rn = 1 and tp.teamposition is not null
    )

select *
from final;
create view "wlm_sales"."productadvisors_agents" as with
    events as (select distinct activeagentid from "DW_WH_200_SILVER_WoodlandMills"."wlm_sales"."productadvisors_events"),

    users as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__systemusers"),

    positions as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"),

    final as (
        select
            e.activeagentid,
            u.username as agentname,
            coalesce(p.teamposition, 'Others') as teamposition
        from events e
        left join users u on e.activeagentid = u.userid
        left join positions p on e.activeagentid = p.userid
        where e.activeagentid is not null
    )
select *
from final;
create view "wlm_sales"."productadvisors_owners" as with
    events as (select distinct ownerid from "DW_WH_200_SILVER_WoodlandMills"."wlm_sales"."productadvisors_events"),

    users as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__systemusers"),

    positions as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"),

    final as (
        select
            e.ownerid,
            u.username as ownername,
            coalesce(p.teamposition, 'Others') as teamposition
        from events e
        left join users u on e.ownerid = u.userid
        left join positions p on e.ownerid = p.userid
    )
select *
from final;
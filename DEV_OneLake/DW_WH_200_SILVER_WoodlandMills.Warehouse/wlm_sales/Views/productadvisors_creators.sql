create view "wlm_sales"."productadvisors_creators" as with
    events as (select distinct createdbyuserid from "DW_WH_200_SILVER_WoodlandMills"."wlm_sales"."productadvisors_events"),

    users as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__systemusers"),

    positions as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"),

    final as (
        select
            e.createdbyuserid,
            u.username as creatorname,
            coalesce(p.teamposition, 'Others') as teamposition
        from events e
        left join users u on e.createdbyuserid = u.userid
        left join positions p on e.createdbyuserid = p.userid
        where e.createdbyuserid is not null
    )
select *
from final;
create view "wlm_sales"."productadvisors_users" as with 
    positions as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"
    ),

    users as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__systemusers"
    ),

    final as (
        select 
            u.userid as ProductAdvisorID,
            u.username as ProductAdvisor,
            coalesce(p.teamposition, 'Others') as TeamPosition
        from users u
        left join positions p on u.userid = p.userid
    )
select *
from final;
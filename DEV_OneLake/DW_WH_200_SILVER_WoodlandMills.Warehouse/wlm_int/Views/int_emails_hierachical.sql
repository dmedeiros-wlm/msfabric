create view "wlm_int"."int_emails_hierachical" as with
    emails as (
        select emailid, activityid, parentactivityid from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__emails"
    ),

    child_counts as (
        select parentactivityid, count(*) as child_count
        from emails
        group by parentactivityid
    ),

    emails_hierarchy as (
        select
            e.emailid,
            e.activityid,
            e.parentactivityid,
            case
                when e.parentactivityid is null and c.child_count > 0
                then 'Root'
                when c.child_count > 0
                then 'Parent'
                when e.parentactivityid is not null and c.child_count is null
                then 'Child'
                when e.parentactivityid is null and c.child_count is null
                then 'Standalone'
            end as category
        from emails e
        left join child_counts c on e.activityid = c.parentactivityid
    )

select emailid, activityid, category
from emails_hierarchy;
create view "wlm_int"."int_emails_deduplicated" as with
    emails as (
        select
            *, row_number() over (partition by emailid order by lastupdated desc) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__emails"
    ),

    final as (
        select
            *,
            case
                when
                    subject = 'Contact Sales Form'
                then 'Sales Contact Us'
                when
                    torecipients like '%sales@woodlandmills.ca%'
                    or torecipients like '%sales@woodlandmills.com%'
                then 'Sales General'
                when
                    subject = 'Contact Tech Form'
                then 'Tech Support Form'
                when 
                    torecipients like '%tech@woodlandmills.com%' 
                then 'Tech General'
                when 
                    torecipients like '%returns@woodlandmills.com%' 
                then 'Return'
                else 'General'
            end as emailtype
        from emails
        where rn = 1 and status = 4
    )

select *
from final;
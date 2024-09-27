create view "wlm_slv"."slv_emails__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Email"),

    emails as (
        select
            Id as emailid,
            subject,
            statuscode as status,
            statecode as state,
            directioncode as direction,
            torecipients,
            activityid,
            parentactivityid,
            ownerid,
            createdon,
            cast(createdon as date) as createdondate,
            createdby as createdbyuserid,
            modifiedon as lastupdated,
            cast(modifiedon as date) as lastupdateddate,
            modifiedby as lastupdatedbyuserid, 
            row_number() over (partition by Id order by modifiedon desc) as rn
        from src
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
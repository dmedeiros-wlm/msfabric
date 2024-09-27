create view "wlm_stg"."stg_dataverse__emails" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Email"),

    final as (
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
            modifiedby as lastupdatedbyuserid
        from src
    )
select *
from final;
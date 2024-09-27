create view "wlm_stg"."stg_dataverse__quotes" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Quote"),

    final as (
        select
            Id as quoteid,
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
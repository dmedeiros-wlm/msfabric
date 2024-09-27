create view "wlm_stg"."stg_dataverse__leads" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Lead"),

    final as (
        select
            Id as leadid,
            ownerid,
            wsi_reasonforcall as reasonforcall,
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
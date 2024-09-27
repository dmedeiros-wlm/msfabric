create view "wlm_stg"."stg_dataverse__opportunities" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Opportunity"),

    final as (
        select
            Id as opportunityid,
            ownerid ,
            wsi_productsinquiringabout as productsinquiringabout,
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
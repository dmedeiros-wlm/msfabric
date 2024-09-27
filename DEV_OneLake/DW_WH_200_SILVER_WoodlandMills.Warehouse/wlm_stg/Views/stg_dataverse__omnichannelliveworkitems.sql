create view "wlm_stg"."stg_dataverse__omnichannelliveworkitems" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."OmnichannelLiveWorkItem"),

    final as (
        select
            Id as chatid,
            msdyn_ocliveworkitemid as conversationid,
            ownerid,
            msdyn_activeagentid as activeagentid,
            msdyn_cdsqueueid as queueid,
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
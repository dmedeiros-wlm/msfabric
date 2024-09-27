create view "wlm_slv"."slv_chats__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."OmnichannelLiveWorkItem"),

    chats as (
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
            modifiedby as lastupdatedbyuserid,
            row_number() over (partition by Id order by modifiedon desc) as rn
        from src
    ),

    final as (
        select
            *,
            case
                when queueid = 'e0baf14a-be37-ee11-bdf4-000d3a09d4ee'
                then 'Sales Queue'
                when queueid = 'f40757f1-bf37-ee11-bdf4-000d3a09d4ee'
                then 'Tech Queue'
                else 'General'
            end as chattype
        from chats
        where rn = 1
    )

select *
from final;
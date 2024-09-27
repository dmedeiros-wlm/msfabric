create view "wlm_slv"."slv_systemusers__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."SystemUser"),

    final as (
        select
            Id as userid,
            fullname as username,
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
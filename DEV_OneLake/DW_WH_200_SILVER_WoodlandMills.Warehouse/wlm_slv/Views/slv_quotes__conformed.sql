create view "wlm_slv"."slv_quotes__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Quote"),

    quotes as (
        select
            Id as quoteid,
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

    final as (select * from quotes where rn = 1)

select *
from final;
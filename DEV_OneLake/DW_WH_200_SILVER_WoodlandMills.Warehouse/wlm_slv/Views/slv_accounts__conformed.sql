create view "wlm_slv"."slv_accounts__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Account"),

    final as (
        select
            Id as accountid,
            accountnumber,
            address1_city as city,
            address1_stateorprovince as stateorprovince,
            address1_country as country,
            address1_postalcode as postalcode,
            wsi_companyname as companyname,
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
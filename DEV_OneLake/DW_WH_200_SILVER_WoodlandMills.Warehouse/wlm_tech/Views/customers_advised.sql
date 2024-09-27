create view "wlm_tech"."customers_advised" as with
    accounts as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_accounts__conformed"),

    incident_accounts as (
        select *
        from accounts
        where
            accountid
            in (select distinct customerid from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_incidents__conformed")
    ),

    final as (
        select
            -- Surrogate key
            convert(
                varchar(64), hashbytes('SHA2_256', accountid), 1
            ) as customersadvised_sk,

            -- Natural key
            accountid,

            -- Dimension attributes
            accountnumber,
            coalesce(city, 'Unknown') as city,
            coalesce(stateorprovince, 'Unknown') as stateorprovince,
            coalesce(country, 'Unknown') country,
            coalesce(postalcode, 'Unknown') as postalcode,
            coalesce(companyname, 'Unknown') as companyname,

            -- Audit attributes
            createdon as audit_createdon,
            createdbyuserid as audit_createdby,
            lastupdated as audit_lastupdated,
            lastupdatedbyuserid as audit_lastupdatedby
        from incident_accounts
    )
select *
from final;
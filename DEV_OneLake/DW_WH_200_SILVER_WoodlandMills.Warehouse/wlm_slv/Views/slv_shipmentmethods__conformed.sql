create view "wlm_slv"."slv_shipmentmethods__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."shipmentmethods"),

    shipmentmethods as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as shipmentmethodid,
            upper(ltrim(rtrim(code))) as shipmentmethodcode,
            upper(ltrim(rtrim(description))) as shipmentmethoddescription,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from shipmentmethods
        where rn = 1
    )

select *
from final;
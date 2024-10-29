create view "wlm_slv"."slv_locations__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."locations"),

    locations as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as locationid,
            upper(ltrim(rtrim(code))) as locationcode,
            upper(ltrim(rtrim(name))) as locationname,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from locations
        where rn = 1
    )

select *
from final;
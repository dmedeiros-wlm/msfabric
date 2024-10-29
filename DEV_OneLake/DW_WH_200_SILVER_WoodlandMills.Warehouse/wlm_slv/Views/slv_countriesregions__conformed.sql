create view "wlm_slv"."slv_countriesregions__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."countriesregions"),

    countriesregions as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as countryid,
            upper(ltrim(rtrim(code))) as countrycode,
            upper(ltrim(rtrim(name))) as countryname,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from countriesregions
        where rn = 1
    )

select *
from final;
create view "wlm_slv"."slv_vendors__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."vendors"),

    vendors as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as vendorid,
            [no] as vendornumber,
            name as vendorname,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from vendors
        where rn = 1
    )

select *
from final;
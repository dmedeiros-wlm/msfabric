create view "wlm_slv"."slv_shippingagents__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."shippingagents"),

    shippingagents as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as shippingagentid,
            upper(ltrim(rtrim(code))) as shippingagentcode,
            upper(ltrim(rtrim(name))) as shippingagentname,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from shippingagents
        where rn = 1
    )

select *
from final;
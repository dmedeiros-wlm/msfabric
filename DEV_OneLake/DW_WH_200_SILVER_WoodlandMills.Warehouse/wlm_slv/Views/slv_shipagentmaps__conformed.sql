create view "wlm_slv"."slv_shipagentmaps__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."shipagentmaps"),

    shipagentmaps as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as shipagentmapid,
            upper(ltrim(rtrim(ediShipper))) as edishipper,
            upper(ltrim(rtrim(shippingAgent))) as shippingagent,
            upper(ltrim(rtrim(tp))) as tradingpartner,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from shipagentmaps
        where rn = 1
    )

select *
from final;
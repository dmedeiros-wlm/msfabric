create view "wlm_slv"."slv_transferlines__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."transferlines"),

    transferlines as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as transferlineid,
            upper(ltrim(rtrim(documentNo))) as transfernumber,
            upper(ltrim(rtrim(itemNo))) as itemnumber,
            quantity,
            qtyToShip as quantitytoship,
            qtyToReceive as quantitytoreceive,
            shipmentDate as shipmentdate,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from transferlines
        where rn = 1
    )

select *
from final;
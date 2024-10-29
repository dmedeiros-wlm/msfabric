create view "wlm_slv"."slv_itemledgerentries__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."itemledgerentries"),

    itemledgerentries as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as itemledgerentryid,
            upper(ltrim(rtrim(documentNo))) as documentnumber,
            entryNo as entrynumber,
            upper(ltrim(rtrim(entryType))) as entrytype,
            upper(ltrim(rtrim(documentType))) as documenttype,
            upper(ltrim(rtrim(orderNo))) as orderno,
            upper(ltrim(rtrim(itemNo))) as itemno,
            upper(ltrim(rtrim(locationCode))) as locationcode,
            quantity,
            invoicedQuantity as invoicedquantity,
            remainingQuantity as remainingquantity,
            salesAmountActual as salesamountactual,
            costAmountActual as costamountactual,
            [open] as isopen,
            postingDate as postingdate,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from itemledgerentries
        where rn = 1
    )

select *
from final;
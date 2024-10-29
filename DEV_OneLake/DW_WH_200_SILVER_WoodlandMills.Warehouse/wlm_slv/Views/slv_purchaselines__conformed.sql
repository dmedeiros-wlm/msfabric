create view "wlm_slv"."slv_purchaselines__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."purchaselines"),

     purchaselines as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as  purchaselineid,
            upper(ltrim(rtrim(documentNo))) as  purchasenumber,
            upper(ltrim(rtrim(type))) as transactiontype,
            upper(ltrim(rtrim([no]))) as itemnumber,
            upper(ltrim(rtrim(description))) as productdescription,
            upper(ltrim(rtrim(genProdPostingGroup))) as genprodpostinggroup,
            upper(ltrim(rtrim(postingGroup))) as postinggroup,
            upper(ltrim(rtrim(locationCode))) as locationcode,
            dropShipment as dropshipment,
            directUnitCost as directunitcost,
            lineAmount as lineamount,
            lineDiscount as linediscount,
            lineDiscountAmount as linediscountamount,
            quantity,
            qtyToReceive as quantitytoreceive,
            quantityReceived as quantityreceived,
            quantityInvoiced as quantityinvoiced,
            upper(ltrim(rtrim(leadTimeCalculation))) as leadtimecalculation,
            orderDate,
            expectedReceiptDate,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from  purchaselines
        where rn = 1
    )

select *
from final;
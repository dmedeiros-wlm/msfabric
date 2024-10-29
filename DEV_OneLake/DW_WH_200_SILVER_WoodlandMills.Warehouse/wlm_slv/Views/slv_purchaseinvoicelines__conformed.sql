create view "wlm_slv"."slv_purchaseinvoicelines__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."purchaseinvoicelines"),

    purchaseinvoicelines as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as purchaseinvoicelineid,
            upper(ltrim(rtrim(documentNo))) as purchaseinvoicenumber,
            upper(ltrim(rtrim(type))) as transactiontype,
            upper(ltrim(rtrim([no]))) as itemnumber,
            upper(ltrim(rtrim(description))) as productdescription,
            quantity,
            lineAmount as lineamount,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from purchaseinvoicelines
        where rn = 1
    )

select *
from final;
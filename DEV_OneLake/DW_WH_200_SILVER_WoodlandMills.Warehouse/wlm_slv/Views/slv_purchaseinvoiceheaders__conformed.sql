create view "wlm_slv"."slv_purchaseinvoiceheaders__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."purchaseinvoiceheaders"),

    purchaseinvoices as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as purchaseinvoiceid,
            upper(ltrim(rtrim([no]))) as purchaseinvoicenumber,
            upper(ltrim(rtrim(buyFromVendorName))) as vendorname,
            upper(ltrim(rtrim(locationCode))) as locationcode,
            upper(ltrim(rtrim(vendorInvoiceNo))) as vendorinvoicenumber,
            upper(ltrim(rtrim(preAssignedNo))) as preassignednumber,
            upper(ltrim(rtrim(currencyCode))) as currencycode,
            postingDate as postingdate,
            dueDate as duedate,
            amount as totalamount,
            amountIncludingVAT as totalamountincludingvat,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from purchaseinvoices
        where rn = 1
    )

select *
from final;
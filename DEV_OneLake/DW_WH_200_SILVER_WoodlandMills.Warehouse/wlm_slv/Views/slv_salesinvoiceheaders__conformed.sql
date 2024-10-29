create view "wlm_slv"."slv_salesinvoiceheaders__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."salesinvoiceheaders"),

    salesinvoices as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as salesinvoiceid,
            upper(ltrim(rtrim([no]))) as salesinvoicenumber,
            upper(ltrim(rtrim(orderNo))) as ordernumber,
            upper(ltrim(rtrim(sellToCustomerNo))) as customernumber,
            upper(ltrim(rtrim(billToAddress))) as billingaddress,
            upper(ltrim(rtrim(billToCity))) as billingcity,
            upper(ltrim(rtrim(billToCounty))) as billingcounty,
            upper(ltrim(rtrim(billToPostCode))) as billingpostalcode,
            upper(ltrim(rtrim(billToCountryRegionCode))) as billingcountryregioncode,
            cancelled as iscancelled,
            amount as totalamount,
            -- invoiceDiscountValue as invoicediscount,
            postingDate as postingdate,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from salesinvoices
        where rn = 1
    )

select *
from final;
create view "wlm_slv"."slv_returnreceiptheaders__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."returnreceiptheaders"),

    returnreceipts as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as returnreceiptid,
            upper(ltrim(rtrim([no]))) as returnreceiptnumber,
            upper(ltrim(rtrim(returnOrderNo))) as returnordernumber,
            upper(ltrim(rtrim(sellToCustomerNo))) as customernumber,
            upper(ltrim(rtrim(billToAddress))) as returningaddress,
            upper(ltrim(rtrim(billToCity))) as returningcity,
            upper(ltrim(rtrim(billToCounty))) as returningcounty,
            upper(ltrim(rtrim(billToPostCode))) as returningpostalcode,
            upper(ltrim(rtrim(billToCountryRegionCode))) as returningcountryregioncode,
            postingDate as postingdate,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from returnreceipts
        where rn = 1
    )

select *
from final;
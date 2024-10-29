create view "wlm_slv"."slv_salesshipmentheaders__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."salesshipmentheaders"),

    salesshipments as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as salesshipmentid,
            upper(ltrim(rtrim([no]))) as salesshipmentnumber,
            upper(ltrim(rtrim(orderNo))) as salesordernumber,
            upper(ltrim(rtrim(sellToCustomerNo))) as customernumber,
            upper(ltrim(rtrim(shipToAddress))) as shippingaddress,
            upper(ltrim(rtrim(shipToCity))) as shippingcity,
            upper(ltrim(rtrim(shipToCounty))) as shippingcounty,
            upper(ltrim(rtrim(shipToPostCode))) as shippingpostalcode,
            upper(ltrim(rtrim(shipToCountryRegionCode))) as shippingcountryregioncode,
            upper(ltrim(rtrim(locationCode))) as locationcode,
            upper(ltrim(rtrim(shipmentMethodCode))) as shipmentmethodcode,
            upper(ltrim(rtrim(shippingAgentCode))) as shippingagentcode,
            upper(ltrim(rtrim(shippingAgentServiceCode))) as shippingagentservicecode,
            wsi0032ExpectedShipping as expectedshipping,
            wsi0042ExpectedDlvryTime as expecteddeliverytime,
            wsi0032TotalCubage as shipmenttotalcubage,
            wsi0032TotalWeight as shipmenttotalweight,
            upper(ltrim(rtrim(currencyCode))) as currencycode,
            currencyFactor as currencyfactor,
            documentDate as documentdate,
            dueDate as duedate,
            orderDate as orderdate,
            postingDate as postingdate,
            requestedDeliveryDate as requesteddeliverydate,
            promisedDeliveryDate as promiseddeliverydate,
            shipmentDate as shipmentdate,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from salesshipments
        where rn = 1
    )

select *
from final;
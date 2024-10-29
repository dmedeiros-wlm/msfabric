create view "wlm_slv"."slv_purchaseheaders__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."purchaseheaders"),

    purchases as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as purchaseid,
            upper(ltrim(rtrim([no]))) as purchasenumber,
            upper(ltrim(rtrim(buyFromVendorNo))) as vendornumber,
            upper(ltrim(rtrim(buyFromVendorName))) as vendorname,
            upper(ltrim(rtrim(vendorInvoiceNo))) as vendorinvoicenumber,
            upper(ltrim(rtrim(yourReference))) as yourreference,
            amount as totalamount,
            amountIncludingVAT as totalamountincludingvat,
            currencyCode as currencycode,
            paymentTermsCode as paymenttermscode,
            wsi0042ProNo as pronumber,
            wsi0042BoLNo as billoflandingnumber,
            wsi0042ContainerNo as containernumber,
            wsi0042SealNo as sealnumber,
            wsi0042ShippingAgent as shippingagent,
            shipmentMethodCode as shipmentmethodcode,
            wsi0032TotalCubage as totalcubage,
            wsi0032TotalWeight as totalweight,
            status,            
            documentDate as documentdate,
            postingDate as postingdate,
            orderDate as orderdate, -- does it make sense to keep this at a header and line level?
            expectedReceiptDate as expectedreceiptdate, -- same as above
            dueDate as duedate,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from purchases
        where rn = 1
    )

select *
from final;
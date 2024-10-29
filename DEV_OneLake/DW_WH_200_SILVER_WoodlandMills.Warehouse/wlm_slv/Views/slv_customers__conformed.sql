create view "wlm_slv"."slv_customers__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."customers"),

    customers as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as customerid,
            upper(ltrim(rtrim([no]))) as customernumber,
            invAmountsLCY as totalamountinvoicedlcy,
            invDiscountsLCY as totaldiscountinvoicedlcy,
            noOfQuotes as numberofquotes,
            noOfOrders as numberoforders,
            noOfReturnOrders as numberofreturns,
            noOfShipToAddresses as numberofshipments,
            noOfPstdCreditMemos as numberofpostedcreditmemos,
            noOfPstdInvoices as numberofpostedinvoices,
            noOfPstdReturnReceipts as numberofpostedreturnreceipts,
            noOfPstdShipments as numberofpostedshipments,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from customers
        where rn = 1
    )

select *
from final;
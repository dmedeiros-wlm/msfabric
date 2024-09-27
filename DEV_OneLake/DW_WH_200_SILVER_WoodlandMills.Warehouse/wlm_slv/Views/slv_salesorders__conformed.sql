create view "wlm_slv"."slv_salesorders__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."SalesOrder"),

    salesorders as (
        select
            Id as salesorderid,
            upper(ltrim(rtrim(ordernumber))) as ordernumber,
            upper(ltrim(rtrim(name))) as ordername,
            upper(ltrim(rtrim(wsi_bcsalesorderno))) as bcordernumber,
            submitdate,
            totalamount,
            wsi_totalweight as totalweight,
            wsi_datesentto3pl as datesentto3pl,
            wsi_account as accountid,
            ownerid,
            opportunityid,
            quoteid,
            pricelevelid as pricelist,
            transactioncurrencyid as currency,
            wsi_originoforder as originoforder,
            statecode as state,
            statuscode as status,
            createdon,
            cast(createdon as date) as createdondate,
            createdby as createdbyuserid,
            modifiedon as lastupdated,
            cast(modifiedon as date) as lastupdateddate,
            modifiedby as lastupdatedbyuserid,
            row_number() over (
                partition by Id order by modifiedon desc
            ) as rn
        from src
    ),

    final as (
        select *,
            case
                when originoforder = '866490000'
                then 'Phone'
                when originoforder = '866490001' and totalamount > 0
                then 'Online Order'
                when originoforder = '866490001' and totalamount = 0
                then 'Free Hat'
                when originoforder = '866490002'
                then 'Walk In'
                when originoforder = '866490003'
                then 'Dealer'
                when originoforder = '866490004'
                then 'Ticket'
                else 'General'
            end as salesordertype
        from salesorders
        where rn = 1
    )

select *
from final;
create view "wlm_stg"."stg_dataverse__salesorders" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."SalesOrder"),

    final as (
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
            modifiedby as lastupdatedbyuserid
        from src
    )
select *
from final;
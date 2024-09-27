create view "wlm_int"."int_salesorders_joined" as with
    salesorders as (
        select
            *,
            row_number() over (
                partition by salesorderid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__salesorders"
    ),

    salesorderdetails as (
        select
            *,
            row_number() over (
                partition by salesorderlineid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__salesorderdetails"
    ),

    final as (
        select
            so.salesorderid,
            so.ordernumber,
            so.ordername,
            so.bcordernumber,
            so.submitdate,
            so.totalamount,
            so.totalweight,
            so.datesentto3pl,
            so.accountid,
            so.ownerid,
            so.opportunityid,
            so.quoteid,
            so.pricelist,
            so.currency,
            so.createdon,
            so.createdondate,
            so.createdbyuserid,
            so.originoforder,
            so.state,
            so.status,
            case
                when so.[lastupdated] > sod.[lastupdated]
                then so.[lastupdated]
                else sod.[lastupdated]
            end as lastupdated,
            case
                when so.[lastupdateddate] > sod.[lastupdateddate]
                then so.[lastupdateddate]
                else sod.[lastupdateddate]
            end as lastupdateddate,
            so.lastupdatedbyuserid,
            sod.salesorderlineid,
            sod.productnumber,
            sod.createdon as linecreatedon,
            sod.createdondate as linecreatedondate,
            sod.createdbyuserid as linecreatedbyuserid,
            sod.lastupdated as linelastupdated,
            sod.lastupdateddate as linelastupdateddate,
            sod.lastupdatedbyuserid as linelastupdatedbyuserid
        from salesorders as so
        left join salesorderdetails as sod on so.salesorderid = sod.salesorderid
        where so.rn = 1 and sod.rn = 1
    )

select *
from final;
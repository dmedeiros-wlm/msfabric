create view "wlm_sales"."productadvisors_events" as with
    leads as (
        select
            leadid as eventid,
            'Lead' as eventtype,
            leadtype as eventsubtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated as LastUpdated,
            null as activeagentid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_leads_deduplicated"
    ),

    opportunities as (
        select
            opportunityid as eventid,
            'Opportunity' as eventtype,
            opportunitytype as eventsubtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated as LastUpdated,
            null as activeagentid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_opportunities_deduplicated"
    ),

    quotes as (
        select
            quoteid as eventid,
            'Quote' as eventtype,
            'General' as eventsubtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated as LastUpdated,
            null as activeagentid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_quotes_deduplicated"
    ),

    chats as (
        select
            chatid as eventid,
            'Chat' as eventtype,
            chattype as eventsubtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated as LastUpdated,
            activeagentid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_chats_deduplicated"
    ),

    phonecalls as (
        select
            phonecallid as eventid,
            'Phone Call' as eventtype,
            phonecalltype as eventsubtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated as LastUpdated,
            null as activeagentid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_phonecalls_deduplicated"
    ),

    emails as (
        select
            emailid as eventid,
            'Email' as eventtype,
            emailtype as eventsubtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated as LastUpdated,
            null as activeagentid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_emails_deduplicated"
        where
            lower(subject) not like 'new order (%'
            and lower(subject) not like 'delivery report%'
            and lower(subject) not like 'new order message for order id%'
            and lower(subject) not like 'notification:%'
            and lower(subject) not like 'sawmilltrader.com%'
            and lower(subject) not like 'security alert%'
            and lower(subject) not like 'shipment confirmation%:%s-shpt%'
            and lower(subject) not like 'your daily highlights from woodland mills%'
            and lower(subject) not like 'your kijiji ad%'
            and lower(subject) not like 'your woodland mills order confirmation%'
            or lower(subject) is null
    ),

    salesorders as (
        select
            salesorderid as eventid,
            'Sales Order' as eventtype,
            salesordertype as eventsubtype,
            createdondate,
            ownerid,
            createdbyuserid,
            lastupdated as LastUpdated,
            null as activeagentid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_int"."int_salesorders_deduplicated"
    ),

    consolidated_events as (
        select *
        from
            (
                select *
                from leads
                union all
                select *
                from opportunities
                union all
                select *
                from quotes
                union all
                select *
                from chats
                union all
                select *
                from phonecalls
                union all
                select *
                from emails
                union all
                select *
                from salesorders
            ) as combined_events
        where createdondate >= '2023-01-01'
    )

select *
from consolidated_events;
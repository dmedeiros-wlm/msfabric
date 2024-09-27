create view "wlm_cx"."customer_touchpoints" as with
    leads as (
        select
            leadid as touchpointid,
            'Lead' as touchpointtype,
            leadtype as touchpointsubtype,
            ownerid,
            'Unknown' as activeagentid,
            createdondate,
            createdon,
            createdbyuserid,
            lastupdated,
            lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_leads__conformed"
    ),

    opportunities as (
        select
            opportunityid as touchpointid,
            'Opportunity' as touchpointtype,
            opportunitytype as touchpointsubtype,
            ownerid,
            'Unknown' as activeagentid,
            createdondate,
            createdon,
            createdbyuserid,
            lastupdated,
            lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_opportunities__conformed"
    ),

    quotes as (
        select
            quoteid as touchpointid,
            'Quote' as touchpointtype,
            'General' as touchpointsubtype,
            ownerid,
            'Unknown' as activeagentid,
            createdondate,
            createdon,
            createdbyuserid,
            lastupdated,
            lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_quotes__conformed"
    ),

    chats as (
        select
            chatid as touchpointid,
            'Chat' as touchpointtype,
            chattype as touchpointsubtype,
            ownerid,
            activeagentid,
            createdondate,
            createdon,
            createdbyuserid,
            lastupdated,
            lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_chats__conformed"
    ),

    phonecalls as (
        select
            phonecallid as touchpointid,
            'Phone Call' as touchpointtype,
            phonecalltype as touchpointsubtype,
            ownerid,
            'Unknown' as activeagentid,
            createdondate,
            createdon,
            createdbyuserid,
            lastupdated,
            lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_phonecalls__conformed"
    ),

    emails as (
        select
            emailid as touchpointid,
            'Email' as touchpointtype,
            emailtype as touchpointsubtype,
            ownerid,
            'Unknown' as activeagentid,
            createdondate,
            createdon,
            createdbyuserid,
            lastupdated,
            lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_emails__conformed"
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
            salesorderid as touchpointid,
            'Sales Order' as touchpointtype,
            salesordertype as touchpointsubtype,
            ownerid,
            'Unknown' as activeagentid,
            createdondate,
            createdon,
            createdbyuserid,
            lastupdated,
            lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_salesorders__conformed"
    ),

    consolidated_touchpoints as (
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
            ) as combined_touchpoints
        where createdondate >= '2023-01-01'
    ),

    final as (
        select
            -- Primary key
            convert(varchar(64), hashbytes('SHA2_256', touchpointid), 1) as touchpoints_pk,

            -- Attribute
            touchpointid,
            ownerid,
            activeagentid,
            createdbyuserid,

            -- Dimension keys
            -- Users
            convert(varchar(64), hashbytes('SHA2_256', ownerid), 1) as owner_fk,
            convert(varchar(64), hashbytes('SHA2_256', activeagentid), 1) as activeagent_fk,
            convert(varchar(64), hashbytes('SHA2_256', createdbyuserid), 1) as creator_fk,

            -- touchpoint Types
            touchpointtype,
            touchpointsubtype,

            -- Date 
            createdondate,
 
            -- Audit attributes
            createdon as audit_createdon,
            createdbyuserid as audit_createdby,
            lastupdated as LastUpdated,
            lastupdatedbyuserid as audit_lastupdatedby
        from consolidated_touchpoints
    )

select *
from final;
create view "wlm_slv"."slv_incidents__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Incident"),

    final as (
        select
            Id as ticketid,
            ownerid,
            customerid,
            wm_escalated as escalatedtoid,
            wm_escalationreason as escalationreason,
            overriddencreatedon as recordcreatedon,
            wsi_ticketresolutiontime as ticketresolutiontime,
            wsi_productsimpacted as productsimpacted,
            casetypecode as tickettypeid,
            prioritycode as priorityid,
            wm_tickettag as tickettagid,
            caseorigincode as originid,
            statuscode as statusreasonid,
            statecode as statusid,
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
create view "wlm_slv"."slv_incidentsresolution__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."IncidentResolution"),

    final as (
        select
            Id as resolutionid,
            incidentid as ticketid,
            resolutiontypecode as resolutiontypeid,
            wsi_resolution as ticketresolutionid,
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
create view "wlm_tech"."resolutions_options" as with
    options as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_options__conformed"),

    -- Incident resolutions - categories
    incident_resolutions as (
        select numericallabel as ticketresolutionid, label as ticketresolution
        from options
        where tablename = 'incidentresolution' and columnname = 'wsi_resolution'
        union 
        select null as ticketresolutionid, 'Unknown' as ticketresolution
    ),

    -- Incident resolutions - type
    incident_resolution_types as (
        select numericallabel as resolutiontypeid, label as resolutiontype
        from options
        where tablename = 'incidentresolution' and columnname = 'resolutiontypecode'
        union 
        select null as resolutiontypeid, 'Unknown' as resolutiontype
    ),

    -- Combine all dimensions into one junk dimension
    combined as (
        select
            -- Hash the natural keys comma-separated string 
            hashbytes(
                'SHA2_256',
                concat(
                    ir.ticketresolutionid,
                    ', ',
                    irt.resolutiontypeid
                )
            ) as hash_key,

            -- Dimensions ids
            ir.ticketresolutionid,
            irt.resolutiontypeid,

            -- Include the labels for each dimension
            ir.ticketresolution,
            irt.resolutiontype
        from incident_resolutions ir
        cross join incident_resolution_types irt
    ),
    
    -- Final Select
    final as (
        select
            -- Surrogate key
            convert(varchar(64), hash_key, 1) as resolutionsoptions_sk,

            -- Natural keys
            ticketresolutionid,
            resolutiontypeid,

            -- Dimension attributes
            ticketresolution,
            resolutiontype
        from combined
    )

select *
from final;
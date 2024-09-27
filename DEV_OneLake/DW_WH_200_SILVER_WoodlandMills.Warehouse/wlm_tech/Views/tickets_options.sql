create view "wlm_tech"."tickets_options" as with
    options as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_options__conformed"),

    global_options as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_globaloptions__conformed"),

    status_reasons as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_statuses__conformed"),

    statuses as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_states__conformed"),

    -- Incidents - origin
    incident_origins as (
        select numericallabel as originid, label as origin
        from global_options
        where tablename = 'incident' and columnname = 'caseorigincode'
        union
        select null as originid, 'Unknown' as origin
    ),

    -- Incidents - priority
    incident_priorities as (
        select numericallabel as priorityid, label as priority
        from options
        where tablename = 'incident' and columnname = 'prioritycode'
        union
        select null as priorityid, 'Unknown' as priority
    ),

    -- Incidents - status
    incident_statuses as (
        select numericallabel as statusid, label as status
        from statuses
        where tablename = 'incident'
        union
        select null as statusid, 'Unknown' as status
    ),

    -- Incidents - statusreason
    incident_statusreasons as (
        select numericallabel as statusreasonid, label as statusreason
        from status_reasons
        where tablename = 'incident'
        union
        select null as statusreasonid, 'Unknown' as statusreason
    ),

    -- Incidents - tag
    incident_tags as (
        select numericallabel as tickettagid, label as tickettag
        from global_options
        where tablename = 'incident' and columnname = 'wm_tickettag'
        union
        select null as tickettagid, 'Unknown' as tickettag
    ),

    -- Incidents - type
    incident_types as (
        select numericallabel as tickettypeid, label as tickettype
        from options
        where
            tablename = 'incident'
            and columnname = 'casetypecode'
        union
        select null as tickettypeid, 'Unknown' as tickettype
    ),

    -- Combine all dimensions into one junk dimension
    combined as (
        select
            -- Hash the natural keys comma-separated string 
            hashbytes(
                'SHA2_256',
                concat(
                    io.originid,
                    ', ',
                    ip.priorityid,
                    ', ',
                    ist.statusid,
                    ', ',
                    isr.statusreasonid,
                    ', ',
                    itg.tickettagid,
                    ', ',
                    itt.tickettypeid
                )
            ) as hash_key,

            -- Dimensions ids
            io.originid,
            ip.priorityid,
            ist.statusid,
            isr.statusreasonid,
            itg.tickettagid,
            itt.tickettypeid,

            -- Include the labels for each dimension
            io.origin,
            ip.priority,
            ist.status,
            isr.statusreason,
            itg.tickettag,
            itt.tickettype
        from incident_origins io
        cross join incident_priorities ip
        cross join incident_statuses ist
        cross join incident_statusreasons isr
        cross join incident_tags itg
        cross join incident_types itt
    ),

    -- Final Select
    final as (
        select
            -- Surrogate key
            convert(varchar(64), hash_key, 1) as ticketsoptions_sk,

            -- Natural keys
            originid,
            priorityid,
            statusid,
            statusreasonid,
            tickettagid,
            tickettypeid,

            -- Dimension attributes
            origin,
            priority,
            status,
            statusreason,
            tickettag,
            tickettype
        from combined
    )

select *
from final;
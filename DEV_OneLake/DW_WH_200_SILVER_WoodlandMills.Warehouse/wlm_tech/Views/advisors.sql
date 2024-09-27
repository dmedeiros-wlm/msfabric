create view "wlm_tech"."advisors" as with
    users as (
        select userid, username, createdon, createdbyuserid, lastupdated, lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_systemusers__conformed"
    ),

    resolutions as (
        select createdbyuserid, lastupdatedbyuserid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_incidentsresolution__conformed"
    ),

    tickets as (
        select createdbyuserid, lastupdatedbyuserid, ownerid, escalatedtoid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_incidents__conformed"
    ),

    team_positions as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_techsupport__teampositions"),

    -- Resolution Representatives
    resolutions_reps as (
        select distinct rp.userid as advisorid, 1 as is_resolution_rep
        from users rp
        where rp.userid in (select distinct createdbyuserid from resolutions)
    ),

    -- Resolution Editors
    resolutions_editors as (
        select distinct ed.userid as advisorid, 1 as is_resolution_editor
        from users ed
        where ed.userid in (select distinct lastupdatedbyuserid from resolutions)
    ),

    -- Ticket Representatives
    incident_reps as (
        select distinct rp.userid as advisorid, 1 as is_ticket_rep
        from users rp
        where rp.userid in (select distinct createdbyuserid from tickets)
    ),

    -- Ticket Editors
    incident_editors as (
        select distinct ed.userid as advisorid, 1 as is_ticket_editor
        from users ed
        where ed.userid in (select distinct lastupdatedbyuserid from tickets)
    ),

    -- Ticket Owners
    incident_owners as (
        select distinct ow.userid as advisorid, 1 as is_ticket_owner
        from users ow
        where ow.userid in (select distinct ownerid from tickets)
    ),

    -- Ticket Escalatees
    incident_escalatees as (
        select distinct es.userid as advisorid, 1 as is_ticket_escalatee
        from users es
        where es.userid in (select distinct escalatedtoid from tickets)
    ),

    -- Combine Roles
    combined_roles as (
        select
            u.userid as advisorid,
            u.username as fullname,
            case
                when
                    upper(ltrim(rtrim(u.userid))) in (
                        select upper(ltrim(rtrim(userid)))
                        from team_positions
                        where teamposition = 'Tech Advisor'
                    )
                then 1
                else 0
            end as is_tech_advisor,
            case
                when
                    upper(ltrim(rtrim(u.userid))) in (
                        select upper(ltrim(rtrim(userid)))
                        from team_positions
                        where teamposition = 'Senior Tech'
                    )
                then 1
                else 0
            end as is_senior_tech,
            case
                when
                    upper(ltrim(rtrim(u.userid))) in (
                        select upper(ltrim(rtrim(userid)))
                        from team_positions
                        where teamposition = 'Team Lead'
                    )
                then 1
                else 0
            end as is_team_lead,
            coalesce(rr.is_resolution_rep, 0) as is_resolution_rep,
            coalesce(re.is_resolution_editor, 0) as is_resolution_editor,
            coalesce(ir.is_ticket_rep, 0) as is_ticket_rep,
            coalesce(ie.is_ticket_editor, 0) as is_ticket_editor,
            coalesce(io.is_ticket_owner, 0) as is_ticket_owner,
            coalesce(ix.is_ticket_escalatee, 0) as is_ticket_escalatee,
            u.createdon,
            u.createdbyuserid,
            u.lastupdated,
            u.lastupdatedbyuserid
        from users u
        left join resolutions_reps rr on u.userid = rr.advisorid
        left join resolutions_editors re on u.userid = re.advisorid
        left join incident_reps ir on u.userid = ir.advisorid
        left join incident_editors ie on u.userid = ie.advisorid
        left join incident_owners io on u.userid = io.advisorid
        left join incident_escalatees ix on u.userid = ix.advisorid

    ),

    final as (
        select distinct
            -- Surrogate key
            convert(varchar(64), hashbytes(
                'SHA2_256',
                advisorid
            ), 1) as advisor_sk,

            -- Natural key
            advisorid,

            -- Dimension attributes
            fullname,
            is_tech_advisor,
            is_senior_tech,
            is_team_lead,
            is_resolution_rep,
            is_resolution_editor,
            is_ticket_rep,
            is_ticket_editor,
            is_ticket_owner,
            is_ticket_escalatee,

            -- Audit attributes
            createdon as audit_createdon,
            createdbyuserid as audit_createdby,
            lastupdated as audit_lastupdated,
            lastupdatedbyuserid as audit_lastupdatedby
        from combined_roles
        where
            is_resolution_rep = 1
            or is_resolution_editor = 1
            or is_ticket_rep = 1
            or is_ticket_editor = 1
            or is_ticket_owner = 1
            or is_ticket_escalatee = 1
    )

select *
from final;
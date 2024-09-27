create view "wlm_cx"."touchpoint_owners" as with
    touchpoints as (select distinct ownerid, owner_fk from "DW_WH_200_SILVER_WoodlandMills"."wlm_cx"."customer_touchpoints"),

    users as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_systemusers__conformed"),

    positions as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_sales__teampositions"),

    final as (
        select
            -- Surrogate key
            e.owner_fk as owner_sk,

            -- Natural key
            e.ownerid,

            -- Dimension attributes
            u.username as agentname,
            coalesce(p.teamposition, 'Others') as teamposition,

            -- Audit attributes
            u.createdon as audit_createdon,
            u.createdbyuserid as audit_createdby,
            u.lastupdated as audit_lastupdated,
            u.lastupdatedbyuserid as audit_lastupdatedby
        from touchpoints e
        left join users u on e.ownerid = u.userid
        left join positions p on e.ownerid = p.userid
        where e.ownerid is not null
    )
select *
from final;
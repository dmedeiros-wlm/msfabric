create view "wlm_fin"."activevendors" as with
    vendors as (
        select
            *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_vendors__conformed"
    ),

    glentries as (select distinct sourcenumber, sourcenumber_fk from "DW_WH_200_SILVER_WoodlandMills"."wlm_fin"."generalledger_entries"),

    final as (
        select
            -- Surrogate key
            e.sourcenumber_fk as sourcenumber_sk,

            -- Natural key
            e.sourcenumber,

            -- Dimension attributes
            v.vendornumber,
            v.vendorname,

            -- Audit attributes
            v.createdon as audit_createdon,
            v.createdbyuserid as audit_createdby,
            v.lastupdated as audit_lastupdated,
            v.lastupdatedbyuserid as audit_lastupdatedby
        from vendors v
        inner join glentries e on v.vendornumber = e.sourcenumber
    )

select *
from final;
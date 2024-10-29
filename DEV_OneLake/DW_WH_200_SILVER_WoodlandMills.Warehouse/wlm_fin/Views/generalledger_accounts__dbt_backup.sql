create view "wlm_fin"."generalledger_accounts" as with
    glaccounts as (
        select
            *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_glaccounts__conformed"
    ),

    glentries as (select distinct glaccountnumber, glaccountnumber_fk from "DW_WH_200_SILVER_WoodlandMills"."wlm_fin"."generalledger_entries"),

    final as (
        select
            -- Surrogate key
            e.glaccountnumber_fk as glaccountnumber_sk,

            -- Natural key
            a.glaccountnumber,

            -- Dimension attributes
            coalesce(a.glaccountname, 'Others') as glaccountname,

            -- Audit attributes
            a.createdon as audit_createdon,
            a.createdbyuserid as audit_createdby,
            a.lastupdated as audit_lastupdated,
            a.lastupdatedbyuserid as audit_lastupdatedby
        from glaccounts a
        inner join glentries e on a.glaccountnumber = e.glaccountnumber
    )

select *
from final;
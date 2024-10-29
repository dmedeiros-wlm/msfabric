create view "wlm_util"."util_finance_glaccountsapprovers" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."glaccountsapprovers"),

    glaccountsapprovers as (
        select
            *
        from src
    )

select *
from glaccountsapprovers;
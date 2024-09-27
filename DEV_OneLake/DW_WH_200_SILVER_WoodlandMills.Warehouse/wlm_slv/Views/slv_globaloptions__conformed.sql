create view "wlm_slv"."slv_globaloptions__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."GlobalOptionsetMetadata"),

    final as (
        select
            EntityName as tablename,
            OptionSetName as columnname,
            [Option] as numericallabel,
            LocalizedLabel as label
        from src
    )
select *
from final;
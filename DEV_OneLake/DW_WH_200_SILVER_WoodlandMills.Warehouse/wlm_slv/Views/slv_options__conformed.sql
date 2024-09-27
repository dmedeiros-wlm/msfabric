create view "wlm_slv"."slv_options__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."OptionsetMetadata"),

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
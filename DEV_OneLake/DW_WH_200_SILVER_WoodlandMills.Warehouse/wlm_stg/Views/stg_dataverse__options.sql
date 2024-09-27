create view "wlm_stg"."stg_dataverse__options" as with
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
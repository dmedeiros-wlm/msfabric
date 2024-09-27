create view "wlm_slv"."slv_statuses__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."StatusMetadata"),

    final as (
        select
            EntityName as tablename,
            Status as numericallabel,
            LocalizedLabel as label
        from src
    )
select *
from final;
create view "wlm_slv"."slv_states__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."StateMetadata"),

    final as (
        select 
            EntityName as tablename,
            State as numericallabel,
            LocalizedLabel as label
        from src
    )
select *
from final;
create view "wlm_int"."int_phonecalls_deduplicated" as with
    phonecalls as (
        select
            *,
            row_number() over (partition by phonecallid order by lastupdated desc) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__phonecalls"
    ),

    final as (
        select
            *,
            case
                when subject like '%Voice Mail%'
                then 'Voice Mail'
                else 'General'
            end as phonecalltype
        from phonecalls
        where rn = 1
    )

select *
from final;
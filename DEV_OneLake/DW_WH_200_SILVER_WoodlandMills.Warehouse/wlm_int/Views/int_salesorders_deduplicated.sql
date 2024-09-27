create view "wlm_int"."int_salesorders_deduplicated" as with
    salesorders as (
        select
            *,
            row_number() over (
                partition by salesorderid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__salesorders"
    ),

    final as (select *,
            case
                when originoforder = '866490000'
                then 'Phone'
                when originoforder = '866490001' and totalamount > 0
                then 'Online Order'
                when originoforder = '866490001' and totalamount = 0
                then 'Free Hat'
                when originoforder = '866490002'
                then 'Walk In'
                when originoforder = '866490003'
                then 'Dealer'
                when originoforder = '866490004'
                then 'Ticket'
                else 'General'
            end as salesordertype from salesorders where rn = 1)

select *
from final;
create view "wlm_ops"."salesshipment" as with
    saleshipments as (select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_salesshipments__conformed" ),
    
    saleshipmentlines as (select salesshipmentid, quantity, postinggroup from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_salesshipmentlines__conformed"),

    joineddata as (
        select
            ss.salesshipmentid,
            sum(ssl.quantity) as total_quantity,
            sum(
                case when ssl.postinggroup = 'INVENTORY' then ssl.quantity else 0 end
            ) as inventory_quantity
        from saleshipments ss
        inner join saleshipmentlines ssl on ss.salesshipmentid = ssl.salesshipmentid
        group by ss.salesshipmentid
    ),

    final as (
        select
            ss.*,
            case
                when
                    (jd.total_quantity > 0 and jd.inventory_quantity = 0)
                    or (jd.total_quantity <= 0)
                then 'Invalid'
                else 'Valid'
            end as isvalidshipment
        from saleshipments ss
        inner join joineddata jd on ss.salesshipmentid = jd.salesshipmentid
    )

select *
from final;
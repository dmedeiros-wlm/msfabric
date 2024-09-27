create view "wlm_tech"."products_impacted" as with
    incident_products as (
        select distinct (s.value) as productid
        from
            "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_incidents__conformed" i
            cross apply string_split(i.productsimpacted, ';') s
    ),

    product_names as (
        select numericallabel as productid, hashbytes(
                'SHA2_256',
                cast(numericallabel as nvarchar)
            ) as hash_key, label as productname
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_options__conformed"
        where columnname = 'wsi_productsimpacted'
    ),

    product_categories as (
        select * from "DW_WH_200_SILVER_WoodlandMills"."wlm_util"."util_techsupport__productcategories"
    ),

    combined as (
        select
            -- Surrogate key
            convert(varchar(64), pn.hash_key, 1) as productsimpacted_sk,

            -- Natural key
            ip.productid,

            -- Dimension attributes
            pn.productname,
            coalesce(pc.productcategory, 'Other') as productcategory
        from incident_products ip
        left join product_names pn on ip.productid = pn.productid
        left join
            product_categories pc
            on upper(pn.productname) like '%' + pc.productnamelike + '%'

    ),

    final as (
        select *
        from combined
        group by productsimpacted_sk, productid, productname, productcategory
    )

select *
from final;
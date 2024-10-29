create view "wlm_slv"."slv_items__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."items"),

    items as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as itemid,
            upper(ltrim(rtrim([no]))) as itemnumber,
            upper(ltrim(rtrim(description))) as itemdescription,
            upper(ltrim(rtrim(itemCategoryCode))) as itemcategorycode,
            upper(ltrim(rtrim(genProdPostingGroup))) as genprodpostinggroup,
            upper(ltrim(rtrim(globalDimension1Code))) as globaldimension1code,
            upper(ltrim(rtrim(globalDimension2Code))) as globaldimension2code,
            grossWeight as unitgrossweight,
            unitVolume as unitvolume,
            unitCost as unitcost,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from items
        where rn = 1
    )

select *
from final;
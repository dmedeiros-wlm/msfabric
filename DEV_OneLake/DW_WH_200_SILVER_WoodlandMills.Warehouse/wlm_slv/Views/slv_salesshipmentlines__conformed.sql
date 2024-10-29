create view "wlm_slv"."slv_salesshipmentlines__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."salesshipmentlines"),

    salesshipmentlines as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as salesshipmentlineid,
            documentId as salesshipmentid,
            upper(ltrim(rtrim(type))) as transactiontype,
            upper(ltrim(rtrim([no]))) as itemnumber,
            upper(ltrim(rtrim(description))) as productdescription,
            upper(ltrim(rtrim(genProdPostingGroup))) as genprodpostinggroup,
            upper(ltrim(rtrim(postingGroup))) as postinggroup,
            upper(ltrim(rtrim(itemCategoryCode))) as itemcategorycode,
            [lineNo] as linenumber,
            quantity,
            unitCost as unitcost,
            unitCostLCY as unitcostlcy,
            unitPrice as unitprice,
            wsi0032Cubage as unitcubage,
            unitVolume as unitvolume,
            wsi0032Weight as unitweight,
            upper(ltrim(rtrim(locationCode))) as locationcode,
            plannedDeliveryDate as planneddeliverydate,
            promisedDeliveryDate as promiseddeliverydate,
            shipmentDate as shipmentdate,
            plannedShipmentDate as plannedshipmentdate,
            postingDate as postingdate,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from salesshipmentlines
        where rn = 1
    )

select *
from final;
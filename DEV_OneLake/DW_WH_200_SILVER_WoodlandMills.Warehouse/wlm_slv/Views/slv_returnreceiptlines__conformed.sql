create view "wlm_slv"."slv_returnreceiptlines__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."returnreceiptlines"),

    returnreceiptlines as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as returnreceiptlineid,
            upper(ltrim(rtrim(documentNo))) as returnreceiptnumber,
            upper(ltrim(rtrim(type))) as transactiontype,
            upper(ltrim(rtrim([no]))) as itemnumber,
            upper(ltrim(rtrim(description))) as productdescription,
            upper(ltrim(rtrim(genProdPostingGroup))) as genprodpostinggroup,
            upper(ltrim(rtrim(postingGroup))) as postinggroup,
            quantity,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from returnreceiptlines
        where rn = 1
    )

select *
from final;
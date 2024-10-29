create view "wlm_slv"."slv_transferheaders__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."transferheaders"),

    transfers as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as transferid,
            upper(ltrim(rtrim([no]))) as transfernumber,
            upper(ltrim(rtrim(transferFromCode))) as transferfromlocationcode,
            upper(ltrim(rtrim(transferToCode))) as transfertolocationcode,
            postingDate as postingdate,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from transfers
        where rn = 1
    )

select *
from final;
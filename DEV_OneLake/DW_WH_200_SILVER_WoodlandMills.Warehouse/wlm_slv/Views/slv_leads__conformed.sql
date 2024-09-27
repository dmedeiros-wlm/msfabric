create view "wlm_slv"."slv_leads__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Lead"),

    leads as (
        select
            Id as leadid,
            ownerid,
            wsi_reasonforcall as reasonforcall,
            createdon,
            cast(createdon as date) as createdondate,
            createdby as createdbyuserid,
            modifiedon as lastupdated,
            cast(modifiedon as date) as lastupdateddate,
            modifiedby as lastupdatedbyuserid,
            row_number() over (partition by Id order by modifiedon desc) as rn
        from src
    ),

    final as (
        select
            *,
            case
                when reasonforcall = '866490003'
                then 'Catalogue Request'
                else 'General'
            end as leadtype
        from leads
        where rn = 1
    )

select *
from final;
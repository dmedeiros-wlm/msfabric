create view "wlm_slv"."slv_opportunities__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Opportunity"),

    opportunities as (
        select
            Id as opportunityid,
            ownerid ,
            wsi_productsinquiringabout as productsinquiringabout,
            createdon,
            cast(createdon as date) as createdondate,
            createdby as createdbyuserid,
            modifiedon as lastupdated,
            cast(modifiedon as date) as lastupdateddate,
            modifiedby as lastupdatedbyuserid,
            row_number() over (
                partition by Id order by modifiedon desc
            ) as rn
        from src
    ),

    final as (
        select
            *,
            case
                when productsinquiringabout = '930820000'
                then 'Catalogue Request'
                else 'General'
            end as opportunitytype
        from opportunities
        where rn = 1
    )

select *
from final;
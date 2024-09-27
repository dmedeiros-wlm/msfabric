create view "wlm_slv"."slv_phonecalls__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."PhoneCall"),

    phonecalls as (
        select
            Id as phonecallid,
            subject,
            statuscode as status,
            statecode as state,
            ownerid,
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
                when subject like '%Voice Mail%'
                then 'Voice Mail'
                else 'General'
            end as phonecalltype
        from phonecalls
        where rn = 1
    )

select *
from final;
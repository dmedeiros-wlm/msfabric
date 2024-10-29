create view "wlm_slv"."slv_glentries__conformed" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."glentries"),

    glentries as (
        select
            *,
            row_number() over (partition by systemId order by systemModifiedAt desc) as rn
        from src
    ),

    final as (
        select
            systemId as glentryid,
            entryNo as entrynumber,
			description as entrydescription,
			documentNo as documentnumber,
			documentType as documenttype,
			postingDate as postingdate,
			gLAccountNo as glaccountnumber,
			debitAmount as debitamount,
			addCurrencyDebitAmount as debitamountlocalcurrency,
			creditAmount as creditamount,
			addCurrencyCreditAmount as creditamountlocalcurrency,
			sourceNo as sourcenumber,
			sourceCode as sourcecode,
            systemCreatedAt as createdon,
            cast(systemCreatedAt as date) as createdondate,
            systemCreatedBy as createdbyuserid,
            systemModifiedAt as lastupdated,
            cast(systemModifiedAt as date) as lastupdateddate,
            systemModifiedBy as lastupdatedbyuserid
        from glentries
        where rn = 1
    )

select *
from final;
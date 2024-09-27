create view "wlm_stg"."stg_dataverse__transactioncurrencies" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."TransactionCurrency"),

    final as (
        select
            Id as currencyid,
            isocurrencycode as currency,
            createdon,
            cast(createdon as date) as createdondate,
            createdby as createdbyuserid,
            modifiedon as lastupdated,
            cast(modifiedon as date) as lastupdateddate,
            modifiedby as lastupdatedbyuserid
        from src
    )
select *
from final;
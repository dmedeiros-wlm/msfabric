create view "wlm_sales"."sales_currency" as with 
    src as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_stg"."stg_dataverse__transactioncurrencies"
    ),

    final as (
        select currencyid as CurrencyID, currency as Currency
        from src
    )
select *
from final;
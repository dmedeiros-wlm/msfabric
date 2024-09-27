create view "wlm_stg"."stg_fabric__calendar" as with
    src as (select * from "DE_LH_100_BRONZE_WoodlandMills"."dbo"."Calendar")

select *
from src;
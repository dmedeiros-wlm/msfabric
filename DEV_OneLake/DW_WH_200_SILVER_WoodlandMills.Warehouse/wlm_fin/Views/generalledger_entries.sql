create view "wlm_fin"."generalledger_entries" as with
    glentries as (
        select *
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_glentries__conformed"
    ),

    final as (
        select
            -- Primary key
            convert(varchar(64), hashbytes('SHA2_256', glentryid), 1) as glentries_pk,

            -- Attribute
            glentryid,
            sourcenumber,
            glaccountnumber,
			documentnumber,

            -- Dimension keys
            convert(varchar(64), hashbytes('SHA2_256', sourcenumber), 1) as sourcenumber_fk,
            convert(varchar(64), hashbytes('SHA2_256', glaccountnumber), 1) as glaccountnumber_fk,
            convert(varchar(64), hashbytes('SHA2_256', documentnumber), 1) as documentnumber_fk,

            -- GL Dimensions
            entrynumber,
			entrydescription,
			documenttype,
            sourcecode,

            -- Metrics
			debitamount,
			debitamountlocalcurrency,
			creditamount,
			creditamountlocalcurrency,
            (creditamount - debitamount) as netamount,
            (creditamountlocalcurrency - debitamountlocalcurrency) as netamountlocalcurrency,

            -- Date 
            postingdate, -- The date when the entry associated document was posted
            createdondate, -- The date when the entry was created
 
            -- Audit attributes
            createdon as audit_createdon,
            createdbyuserid as audit_createdby,
            lastupdated as audit_lastupdated,
            lastupdatedbyuserid as audit_lastupdatedby,

            -- Incremental refresh timestamp
            lastupdated as LastUpdated
        from glentries
    )

select *
from final;
create view "wlm_tech"."tickets" as with
    rankedincidents as (
        select
            *,
            row_number() over (
                partition by ticketid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_incidents__conformed"
    ),

    mappedproducts as (
        select 
            i.ticketid,
            s.value as productid
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_incidents__conformed" i
            cross apply string_split(i.productsimpacted, ';') s
    ),

    rankedincidentresolutions as (
        select
            *,
            row_number() over (
                partition by resolutionid order by lastupdated desc
            ) as rn
        from "DW_WH_200_SILVER_WoodlandMills"."wlm_slv"."slv_incidentsresolution__conformed"
    ),

    joined as (
        select
            hashbytes(
                'SHA2_256',
                concat(ri.ticketid, ', ', mp.productid, ', ', ir.resolutionid)
            ) as tickets_pk,

            ri.ticketid,
            mp.productid,
            ir.resolutionid,

            hashbytes(
                'SHA2_256',
                cast(mp.productid as nvarchar)
            ) as product_fk,

            hashbytes(
                'SHA2_256',
                ri.customerid
            ) as customer_fk,

            hashbytes(
                'SHA2_256',
                concat(
                    ir.ticketresolutionid,
                    ', ',
                    ir.resolutiontypeid
                )
            ) as resolutionoptions_fk,

            hashbytes(
                'SHA2_256',
                concat(
                    ri.originid,
                    ', ',
                    ri.priorityid,
                    ', ',
                    ri.statusid,
                    ', ',
                    ri.statusreasonid,
                    ', ',
                    ri.tickettagid,
                    ', ',
                    ri.tickettypeid
                )
            ) as ticketoptions_fk,

            hashbytes(
                'SHA2_256',
                ir.createdbyuserid
            ) as resolutionrep_fk,

            hashbytes(
                'SHA2_256',
                ir.lastupdatedbyuserid
            ) as resolutioneditor_fk,

            hashbytes(
                'SHA2_256',
                ri.ownerid
            ) as owner_fk,

            hashbytes(
                'SHA2_256',
                ri.createdbyuserid
            ) as rep_fk,

            hashbytes(
                'SHA2_256',
                ri.lastupdatedbyuserid
            ) as editor_fk,

            hashbytes(
                'SHA2_256',
                ri.escalatedtoid
            ) as escalatee_fk,

            ri.ticketresolutiontime,
            ri.escalationreason,
            ri.recordcreatedon,
            ri.createdon,
            ri.createdondate,
            case
                when ri.[lastupdated] > ir.lastupdated or ir.lastupdateddate is null
                then ri.[lastupdated]
                else ir.lastupdated
            end as LastUpdated,
            case
                when ri.[lastupdateddate] > ir.lastupdateddate or ir.lastupdateddate is null
                then ri.[lastupdateddate]
                else ir.lastupdateddate
            end as lastupdateddate,
            ir.createdon as resolutioncreatedon,
            ir.createdondate as resolutioncreatedondate,
            ir.lastupdated as resolutionlastupdated,
            ir.lastupdateddate as resolutionlastupdateddate
        from rankedincidents ri
            left join mappedproducts mp on ri.ticketid = mp.ticketid
            left join rankedincidentresolutions ir on ri.ticketid = ir.ticketid
        where ri.rn = 1
    ),

    final as (
        select
            -- Composite key
            convert(varchar(64), tickets_pk, 1) as tickets_pk,

            -- Attributes
            ticketid,
            productid,
            resolutionid,

            -- Dimension keys
            -- DK Products
            convert(varchar(64), product_fk, 1) as product_fk,

            -- DK Customers
            convert(varchar(64), customer_fk, 1) as customer_fk,
            
            -- Junk DK Options
            convert(varchar(64), resolutionoptions_fk, 1) as resolutionoptions_fk,
            convert(varchar(64), ticketoptions_fk, 1) as ticketoptions_fk,

            -- Role-play DK Advisors
            convert(varchar(64), resolutionrep_fk, 1) as resolutionrep_fk,
            convert(varchar(64), resolutioneditor_fk, 1) as resolutioneditor_fk,
            convert(varchar(64), owner_fk, 1) as owner_fk,
            convert(varchar(64), rep_fk, 1) as rep_fk,
            convert(varchar(64), editor_fk, 1) as editor_fk,
            convert(varchar(64), escalatee_fk, 1) as escalatee_fk,

            -- Metric
            ticketresolutiontime,
            escalationreason,

            -- Audit
            recordcreatedon,
            createdon,
            createdondate,
            LastUpdated,
            lastupdateddate,
            resolutioncreatedon,
            resolutioncreatedondate,
            resolutionlastupdated,
            resolutionlastupdateddate
        from joined
    -- where createdondate > '2023-01-01'
    )
select *
from final;
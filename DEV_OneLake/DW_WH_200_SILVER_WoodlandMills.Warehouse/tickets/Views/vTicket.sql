CREATE   VIEW [tickets].[vTicket] AS WITH RankedIncidents AS (
    SELECT Id AS TicketID,
        ownerid AS OwnerID,
        customerid AS CustomerID,
        wm_escalated AS EscalatedToID,
        wm_escalationreason AS EscalationReason,
        overriddencreatedon AS RecordCreatedOn,
        wsi_ticketresolutiontime AS TicketResolutionTime,
        createdon AS CreatedOn,
        CAST(createdon AS DATE) AS CreatedOnDate,
        createdby AS RepID,
        modifiedon AS LastUpdated,
        CAST(modifiedon AS DATE) AS LastUpdatedDate,
        modifiedby AS EditorID,
        casetypecode AS TicketTypeID,
        prioritycode AS PriorityID,
        wm_tickettag AS TicketTagID,
        caseorigincode AS OriginID,
        statuscode AS StatusReasonID,
        statecode AS StatusID,
        ROW_NUMBER() OVER (
            PARTITION BY Id
            ORDER BY modifiedon DESC
        ) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Incident]
),
MappedProducts AS (
    SELECT i.Id AS TicketID,
        s.value AS ProductID
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[Incident] i
        CROSS APPLY STRING_SPLIT(i.wsi_productsimpacted, ';') s
),
RankedIncidentResolutions AS (
    SELECT incidentid AS TicketID,
        Id as ResolutionID,
        createdon AS ResolutionCreatedOn,
        CAST(createdon AS DATE) AS ResolutionCreatedOnDate,
        createdby AS ResolutionRepID,
        modifiedon AS ResolutionLastUpdated,
        CAST(modifiedon AS DATE) AS ResolutionLastUpdatedDate,
        modifiedby AS ResolutionEditorID,
        resolutiontypecode AS ResolutionTypeID,
        wsi_resolution AS TicketResolutionID,
        ROW_NUMBER() OVER (
            PARTITION BY Id
            ORDER BY modifiedon DESC
        ) AS rn
    FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[IncidentResolution]
)
SELECT
    CONCAT(
        ri.TicketID,
        '-',
        mp.ProductID,
        '-',
        ir.ResolutionID
    ) AS CompositeKey,
    ri.TicketID,
    ri.CustomerID,
    ri.OwnerID,
    ri.RepID,
    ri.EditorID,
    ir.ResolutionID,
    ir.ResolutionRepID,
    ir.ResolutionEditorID,
    ri.EscalatedToID,
    ri.TicketTypeID,
    ri.PriorityID,
    ri.TicketTagID,
    ri.OriginID,
    ri.StatusReasonID,
    ri.StatusID,
    ir.TicketResolutionID,
    ir.ResolutionTypeID,
    mp.ProductID,
    ri.EscalationReason,
    ri.TicketResolutionTime,
    ri.RecordCreatedOn,
    ri.CreatedOn,
    ri.CreatedOnDate,
    CASE
        WHEN ri.[LastUpdated] > ir.ResolutionLastUpdated or ir.ResolutionLastUpdated is null THEN ri.[LastUpdated]
        ELSE ir.ResolutionLastUpdated
    END AS LastUpdated,
    CASE
        WHEN ri.[LastUpdatedDate] > ir.ResolutionLastUpdatedDate or ir.ResolutionLastUpdated is null THEN ri.[LastUpdatedDate]
        ELSE ir.ResolutionLastUpdatedDate
    END AS LastUpdatedDate,
    ir.ResolutionCreatedOn,
    ir.ResolutionCreatedOnDate,
    ir.ResolutionLastUpdated,
    ir.ResolutionLastUpdatedDate
FROM RankedIncidents ri
    LEFT OUTER JOIN MappedProducts mp ON ri.TicketID = mp.TicketID
    LEFT OUTER JOIN RankedIncidentResolutions ir ON ri.TicketID = ir.TicketID
WHERE ri.rn = 1
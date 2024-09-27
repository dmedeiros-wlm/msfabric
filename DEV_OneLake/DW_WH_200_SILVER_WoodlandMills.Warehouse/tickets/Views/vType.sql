CREATE   VIEW [tickets].[vType]
AS
SELECT DISTINCT
    ctm.[Option] AS TicketTypeID,
    COALESCE(ctm.LocalizedLabel, '') AS TicketType
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[OptionsetMetadata] ctm
WHERE ctm.EntityName = 'incident' AND ctm.OptionSetName = 'casetypecode';
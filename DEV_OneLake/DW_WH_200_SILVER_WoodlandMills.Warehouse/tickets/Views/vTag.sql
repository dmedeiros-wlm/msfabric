CREATE   VIEW [tickets].[vTag]
AS
SELECT DISTINCT
    ttm.[Option] AS TicketTagID,
    COALESCE(ttm.LocalizedLabel, '') AS TicketTag
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[GlobalOptionsetMetadata] ttm
WHERE ttm.EntityName = 'incident' AND ttm.OptionSetName = 'wm_tickettag';
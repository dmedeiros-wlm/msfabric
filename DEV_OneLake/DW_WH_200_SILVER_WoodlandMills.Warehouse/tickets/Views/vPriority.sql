CREATE   VIEW [tickets].[vPriority]
AS
SELECT DISTINCT
    ptm.[Option] AS PriorityID,
    COALESCE(ptm.LocalizedLabel, '') AS Priority
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[OptionsetMetadata] ptm
WHERE ptm.EntityName = 'incident' AND ptm.OptionSetName = 'prioritycode';
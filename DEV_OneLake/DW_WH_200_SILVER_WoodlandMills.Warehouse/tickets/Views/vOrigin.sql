CREATE   VIEW [tickets].[vOrigin]
AS
SELECT DISTINCT
    com.[Option] AS OriginID,
    COALESCE(com.LocalizedLabel, '') AS Origin
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[GlobalOptionsetMetadata] com
WHERE com.EntityName = 'incident' AND com.OptionSetName = 'caseorigincode';
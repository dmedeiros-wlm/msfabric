CREATE   VIEW [tickets].[vResolution]
AS
SELECT DISTINCT
    rem.[Option] AS ResolutionID,
    COALESCE(rem.LocalizedLabel, '') AS Resolution
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[OptionsetMetadata] rem
WHERE rem.EntityName = 'incidentresolution' AND rem.OptionSetName = 'wsi_resolution';
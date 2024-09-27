CREATE   VIEW [tickets].[vResolutionType]
AS
SELECT DISTINCT
    rcm.[Option] AS ResolutionTypeID,
    COALESCE(rcm.LocalizedLabel, '') AS ResolutionTypeCode
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[OptionsetMetadata] rcm
WHERE rcm.EntityName = 'incidentresolution' AND rcm.OptionSetName = 'resolutiontypecode';
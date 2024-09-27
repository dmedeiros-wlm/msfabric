CREATE   VIEW [tickets].[vStatusReason]
AS
SELECT DISTINCT
    stm.[Status] AS StatusReasonID,
    COALESCE(stm.LocalizedLabel, '') AS StatusReason
FROM [DE_LH_100_BRONZE_WoodlandMills].[dbo].[StatusMetadata] stm
WHERE stm.EntityName = 'incident';
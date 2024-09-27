CREATE PROCEDURE [dbo].[UpdateGoldPipelineStats]
    @json VARCHAR(MAX)
AS
BEGIN
    -- Step 1: Drop the temporary table if it exists
    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DW_WH_MetadataOrchestration].[dbo].[TempGoldStats]') AND type in (N'U'))
    
    DROP TABLE [DW_WH_MetadataOrchestration].[dbo].[TempGoldStats];
    
    -- Step 2: Recreate temporary table if it doesn't exist
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DW_WH_MetadataOrchestration].[dbo].[TempGoldStats]') AND type in (N'U'))
    
    CREATE TABLE [DW_WH_MetadataOrchestration].[dbo].[TempGoldStats] (
        loadtype VARCHAR(15),
        batchloaddatetime DATETIME2(6),
        loadstatus VARCHAR(15),
        rowsread INT,
        rowscopied INT,
        deltalakeinserted INT,
        deltalakeupdated INT,
        sinkmaxdatetime DATETIME2(6),
        sourcestartdate DATETIME2(6),
        pipelinestarttime DATETIME2(6),
        pipelineendtime DATETIME2(6),
        sourceschema VARCHAR(100),
        sourcetable VARCHAR(50)
    );

    -- Step 3: Insert the JSON data into the temporary table
    INSERT INTO [DW_WH_MetadataOrchestration].[dbo].[TempGoldStats]
    SELECT *
    FROM OPENJSON(@json)
    WITH (
        loadtype VARCHAR(15) '$.tablestats.loadtype',
        batchloaddatetime DATETIME2(6) '$.tablestats.batchloaddatetime',
        loadstatus VARCHAR(15) '$.tablestats.loadstatus',
        rowsread INT '$.tablestats.rowsread',
        rowscopied INT '$.tablestats.rowscopied',
        deltalakeinserted INT '$.tablestats.deltalakeinserted',
        deltalakeupdated INT '$.tablestats.deltalakeupdated',
        sinkmaxdatetime DATETIME2(6) '$.tablestats.sinkmaxdatetime',
        sourcestartdate DATETIME2(6) '$.tablestats.sourcestartdate',
        pipelinestarttime DATETIME2(6) '$.tablestats.pipelinestarttime',
        pipelineendtime DATETIME2(6) '$.tablestats.pipelineendtime',
        sourceschema VARCHAR(100) '$.tablestats.sourceschema',
        sourcetable VARCHAR(50) '$.tablestats.sourcetable'
    );

    -- Step 4: Perform the bulk update
    UPDATE B
    SET 
        B.batchloaddatetime = T.batchloaddatetime,
        B.loadstatus = T.loadstatus,
        B.rowsread = T.rowsread,
        B.rowscopied = T.rowscopied,
        B.deltalakeinserted = T.deltalakeinserted,
        B.deltalakeupdated = CASE WHEN T.loadtype = 'incremental' THEN T.deltalakeupdated ELSE B.deltalakeupdated END,
        B.sinkmaxdatetime = CASE WHEN T.loadtype = 'incremental' THEN T.sinkmaxdatetime ELSE B.sinkmaxdatetime END,
        B.sourcestartdate = CASE WHEN T.loadtype = 'incremental' THEN T.sourcestartdate ELSE B.sourcestartdate END,
        B.pipelinestarttime = T.pipelinestarttime,
        B.pipelineendtime = T.pipelineendtime
    FROM [DW_WH_MetadataOrchestration].[dbo].[PipelineOrchestrator_Gold] B
    JOIN [DW_WH_MetadataOrchestration].[dbo].[TempGoldStats] T
    ON B.sourceschema = T.sourceschema
    AND B.sourcetable = T.sourcetable;

END;
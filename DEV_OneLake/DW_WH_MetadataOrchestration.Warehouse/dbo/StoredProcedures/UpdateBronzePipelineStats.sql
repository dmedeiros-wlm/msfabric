CREATE PROCEDURE [dbo].[UpdateBronzePipelineStats]
    @json VARCHAR(MAX)
AS
BEGIN
    -- Step 1: Drop the temporary table if it exists
    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DW_WH_MetadataOrchestration].[dbo].[TempBronzeStats]') AND type in (N'U'))
    
    DROP TABLE [DW_WH_MetadataOrchestration].[dbo].[TempBronzeStats];
    
    -- Step 2: Recreate temporary table if it doesn't exist
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DW_WH_MetadataOrchestration].[dbo].[TempBronzeStats]') AND type in (N'U'))
    
    CREATE TABLE [DW_WH_MetadataOrchestration].[dbo].[TempBronzeStats] (
        loadtype VARCHAR(15),
        batchloaddatetime DATETIME2(6),
        loadstatus VARCHAR(15),
        rowsread INT,
        rowscopied INT,
        deltalakeinserted INT,
        deltalakeupdated INT,
        ingestmaxdatetime DATETIME2(6),
        ingeststartdate DATETIME2(6),
        pipelinestarttime DATETIME2(6),
        pipelineendtime DATETIME2(6),
        ingestsourceschema VARCHAR(100),
        ingestsourcetable VARCHAR(50)
    );

    -- Step 3: Insert the JSON data into the temporary table
    INSERT INTO [DW_WH_MetadataOrchestration].[dbo].[TempBronzeStats]
    SELECT *
    FROM OPENJSON(@json)
    WITH (
        loadtype VARCHAR(15) '$.tablestats.loadtype',-- OK
        batchloaddatetime DATETIME2(6) '$.tablestats.batchloaddatetime',-- OK
        loadstatus VARCHAR(15) '$.tablestats.loadstatus',-- OK
        rowsread INT '$.tablestats.rowsread',-- OK
        rowscopied INT '$.tablestats.rowscopied',-- OK
        deltalakeinserted INT '$.tablestats.deltalakeinserted',-- OK
        deltalakeupdated INT '$.tablestats.deltalakeupdated',-- OK
        ingestmaxdatetime DATETIME2(6) '$.tablestats.ingestmaxdatetime',-- OK
        ingeststartdate DATETIME2(6) '$.tablestats.ingeststartdate',-- OK
        pipelinestarttime DATETIME2(6) '$.tablestats.pipelinestarttime',-- OK
        pipelineendtime DATETIME2(6) '$.tablestats.pipelineendtime',-- OK
        ingestsourceschema VARCHAR(100) '$.tablestats.ingestsourceschema',-- OK
        ingestsourcetable VARCHAR(50) '$.tablestats.ingestsourcetable'-- OK
    );

    -- Step 4: Perform the bulk update
    UPDATE B
    SET 
        B.batchloaddatetime = T.batchloaddatetime,
        B.loadstatus = T.loadstatus,
        B.rowsread = T.rowsread,
        B.rowscopied = T.rowscopied,
        B.deltalakeinserted = T.deltalakeinserted,
        B.deltalakeupdated = T.deltalakeupdated,
        B.ingestmaxdatetime = T.ingestmaxdatetime,
        B.ingeststartdate = CASE WHEN T.loadtype = 'incremental' THEN T.ingeststartdate ELSE B.ingeststartdate END,
        B.pipelinestarttime = T.pipelinestarttime,
        B.pipelineendtime = T.pipelineendtime
    FROM [DW_WH_MetadataOrchestration].[dbo].[PipelineOrchestrator_Bronze] B
    JOIN [DW_WH_MetadataOrchestration].[dbo].[TempBronzeStats] T
    ON B.ingestsourceschema = T.ingestsourceschema
    AND B.ingestsourcetable = T.ingestsourcetable;

END;
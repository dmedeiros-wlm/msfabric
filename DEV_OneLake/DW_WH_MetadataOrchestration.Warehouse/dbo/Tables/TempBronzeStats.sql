CREATE TABLE [dbo].[TempBronzeStats] (

	[loadtype] varchar(15) NULL, 
	[batchloaddatetime] datetime2(6) NULL, 
	[loadstatus] varchar(15) NULL, 
	[rowsread] int NULL, 
	[rowscopied] int NULL, 
	[deltalakeinserted] int NULL, 
	[deltalakeupdated] int NULL, 
	[ingestmaxdatetime] datetime2(6) NULL, 
	[ingeststartdate] datetime2(6) NULL, 
	[pipelinestarttime] datetime2(6) NULL, 
	[pipelineendtime] datetime2(6) NULL, 
	[ingestsourceschema] varchar(100) NULL, 
	[ingestsourcetable] varchar(50) NULL
);


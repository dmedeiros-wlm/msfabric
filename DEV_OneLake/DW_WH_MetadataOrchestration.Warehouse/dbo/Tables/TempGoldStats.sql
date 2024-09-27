CREATE TABLE [dbo].[TempGoldStats] (

	[loadtype] varchar(15) NULL, 
	[batchloaddatetime] datetime2(6) NULL, 
	[loadstatus] varchar(15) NULL, 
	[rowsread] int NULL, 
	[rowscopied] int NULL, 
	[deltalakeinserted] int NULL, 
	[deltalakeupdated] int NULL, 
	[sinkmaxdatetime] datetime2(6) NULL, 
	[sourcestartdate] datetime2(6) NULL, 
	[pipelinestarttime] datetime2(6) NULL, 
	[pipelineendtime] datetime2(6) NULL, 
	[sourceschema] varchar(100) NULL, 
	[sourcetable] varchar(50) NULL
);


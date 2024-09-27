CREATE TABLE [dbo].[PipelineOrchestrator_Bronze] (

	[pipelinename] varchar(100) NOT NULL, 
	[ingestsourceschema] varchar(100) NOT NULL, 
	[ingestsourcetable] varchar(50) NOT NULL, 
	[ingestsourcedatecolumn] varchar(50) NULL, 
	[sourcekeycolumn] varchar(50) NULL, 
	[ingeststartdate] datetime2(6) NULL, 
	[ingestenddate] datetime2(6) NULL, 
	[sinktablename] varchar(100) NULL, 
	[loadtype] varchar(15) NOT NULL, 
	[skipload] bit NOT NULL, 
	[batchloaddatetime] datetime2(6) NULL, 
	[loadstatus] varchar(15) NULL, 
	[rowsread] int NULL, 
	[rowscopied] int NULL, 
	[deltalakeinserted] int NULL, 
	[deltalakeupdated] int NULL, 
	[ingestmaxdatetime] datetime2(6) NULL, 
	[pipelinestarttime] datetime2(6) NULL, 
	[pipelineendtime] datetime2(6) NULL
);


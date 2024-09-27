CREATE TABLE [dbo].[PipelineOrchestrator_Gold] (

	[pipelinename] varchar(100) NOT NULL, 
	[sourceschema] varchar(100) NOT NULL, 
	[sourcetable] varchar(50) NOT NULL, 
	[sourcestartdate] datetime2(6) NULL, 
	[sourceenddate] datetime2(6) NULL, 
	[sinktable] varchar(100) NULL, 
	[loadtype] varchar(15) NOT NULL, 
	[tablekey] varchar(50) NULL, 
	[tablekey2] varchar(50) NULL, 
	[skipload] bit NOT NULL, 
	[batchloaddatetime] datetime2(6) NULL, 
	[loadstatus] varchar(15) NULL, 
	[rowsread] int NULL, 
	[rowscopied] char(10) NULL, 
	[deltalakeupdated] int NULL, 
	[deltalakeinserted] int NULL, 
	[sinkmaxdatetime] datetime2(6) NULL, 
	[pipelinestarttime] datetime2(6) NULL, 
	[pipelineendtime] datetime2(6) NULL
);


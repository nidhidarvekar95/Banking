CREATE TABLE [dbo].[dq_results] (

	[table_name] varchar(8000) NULL, 
	[rule_name] varchar(8000) NULL, 
	[row_count] bigint NULL, 
	[batch_id] varchar(8000) NULL, 
	[execution_ts] datetime2(6) NULL
);
CREATE TABLE [dbo].[dim_branch] (

	[branch_key] bigint NULL, 
	[branch_id] varchar(max) NULL, 
	[branch_name] varchar(max) NULL, 
	[city] varchar(max) NULL, 
	[state] varchar(max) NULL, 
	[region] varchar(max) NULL, 
	[opened_date] date NULL, 
	[is_active] int NULL
);
CREATE TABLE [dbo].[dim_account] (

	[account_key] bigint NULL, 
	[account_id] varchar(max) NULL, 
	[customer_id] varchar(max) NULL, 
	[branch_id] varchar(max) NULL, 
	[account_type] varchar(max) NULL, 
	[currency] varchar(max) NULL, 
	[open_date] date NULL, 
	[status] varchar(max) NULL, 
	[overdraft_limit] decimal(18,2) NULL, 
	[record_source] varchar(max) NULL, 
	[load_ts] datetime2(6) NULL
);
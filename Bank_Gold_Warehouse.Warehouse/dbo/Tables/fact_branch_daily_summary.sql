CREATE TABLE [dbo].[fact_branch_daily_summary] (

	[date_key] int NULL, 
	[branch_key] bigint NULL, 
	[total_balance] decimal(28,2) NULL, 
	[total_accounts] bigint NULL, 
	[total_customers] bigint NULL
);
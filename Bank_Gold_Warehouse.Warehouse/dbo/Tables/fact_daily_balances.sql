CREATE TABLE [dbo].[fact_daily_balances] (

	[account_key] bigint NULL, 
	[customer_key] bigint NULL, 
	[branch_key] bigint NULL, 
	[date_key] int NULL, 
	[snapshot_date] date NULL, 
	[end_of_day_balance] decimal(18,2) NULL, 
	[currency] varchar(max) NULL
);
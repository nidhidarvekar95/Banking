CREATE TABLE [dbo].[fact_transactions] (

	[account_key] bigint NULL, 
	[branch_key] bigint NULL, 
	[channel_key] bigint NULL, 
	[counterparty] varchar(max) NULL, 
	[currency] varchar(max) NULL, 
	[customer_key] bigint NULL, 
	[date_key] int NULL, 
	[direction] varchar(max) NULL, 
	[is_fraud_label] int NULL, 
	[merchant_category] varchar(max) NULL, 
	[transaction_id] varchar(max) NULL, 
	[transaction_ts] datetime2(6) NULL, 
	[txn_type_key] bigint NULL, 
	[amount] decimal(18,2) NULL
);
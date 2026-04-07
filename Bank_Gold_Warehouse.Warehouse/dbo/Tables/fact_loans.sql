CREATE TABLE [dbo].[fact_loans] (

	[loan_key] bigint NULL, 
	[date_key] int NULL, 
	[customer_key] bigint NULL, 
	[branch_key] bigint NULL, 
	[principal_amount] decimal(18,2) NULL, 
	[interest_rate_apr] decimal(10,4) NULL, 
	[tenure_months] int NULL, 
	[status] varchar(max) NULL
);
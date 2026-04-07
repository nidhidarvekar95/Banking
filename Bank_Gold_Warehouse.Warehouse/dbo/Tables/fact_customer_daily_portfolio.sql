CREATE TABLE [dbo].[fact_customer_daily_portfolio] (

	[customer_key] bigint NULL, 
	[date_key] int NULL, 
	[loan_count] bigint NULL, 
	[total_principal_amount] decimal(28,2) NULL, 
	[avg_interest_rate_apr] decimal(14,8) NULL, 
	[avg_tenure_months] float NULL
);
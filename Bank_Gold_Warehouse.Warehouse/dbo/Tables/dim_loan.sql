CREATE TABLE [dbo].[dim_loan] (

	[loan_key] bigint NULL, 
	[loan_id] varchar(max) NULL, 
	[customer_id] varchar(max) NULL, 
	[branch_id] varchar(max) NULL, 
	[loan_type] varchar(max) NULL, 
	[start_date] date NULL, 
	[tenure_months] int NULL, 
	[principal_amount] decimal(18,2) NULL, 
	[interest_rate_apr] float NULL, 
	[status] varchar(max) NULL
);
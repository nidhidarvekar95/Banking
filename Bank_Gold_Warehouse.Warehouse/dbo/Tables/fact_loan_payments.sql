CREATE TABLE [dbo].[fact_loan_payments] (

	[branch_key] bigint NULL, 
	[customer_key] bigint NULL, 
	[date_key] int NULL, 
	[loan_key] bigint NULL, 
	[payment_amount] decimal(19,2) NULL, 
	[payment_id] varchar(max) NULL, 
	[status] varchar(max) NULL
);
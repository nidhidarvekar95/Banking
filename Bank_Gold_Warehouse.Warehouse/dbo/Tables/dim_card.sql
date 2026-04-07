CREATE TABLE [dbo].[dim_card] (

	[card_key] bigint NOT NULL, 
	[card_id] varchar(max) NULL, 
	[customer_id] varchar(max) NULL, 
	[linked_account_id] varchar(max) NULL, 
	[card_type] varchar(max) NULL, 
	[status] varchar(max) NULL, 
	[issued_date] date NULL, 
	[credit_limit] decimal(18,2) NULL
);
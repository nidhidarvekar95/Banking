CREATE TABLE [dbo].[dim_customer] (

	[customer_key] bigint NULL, 
	[customer_id] varchar(max) NULL, 
	[full_name] varchar(max) NULL, 
	[gender] varchar(max) NULL, 
	[dob] date NULL, 
	[city] varchar(max) NULL, 
	[state] varchar(max) NULL, 
	[segment] varchar(max) NULL, 
	[risk_rating] varchar(max) NULL, 
	[home_branch_id] varchar(max) NULL, 
	[kyc_status] varchar(max) NULL, 
	[record_source] varchar(max) NULL, 
	[load_ts] datetime2(6) NULL
);
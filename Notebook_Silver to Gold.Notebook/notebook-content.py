# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b87fc055-5393-41e8-8bef-b8a08520a62f",
# META       "default_lakehouse_name": "Bank_Silver_Lakehouse",
# META       "default_lakehouse_workspace_id": "39d0b856-8252-47ff-a893-808b73f74d8c",
# META       "known_lakehouses": [
# META         {
# META           "id": "b87fc055-5393-41e8-8bef-b8a08520a62f"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "949d7c0d-cb1c-981a-4fb1-fa9443370c1f",
# META       "known_warehouses": [
# META         {
# META           "id": "949d7c0d-cb1c-981a-4fb1-fa9443370c1f",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Imports**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Silver Tables**

# CELL ********************

from pyspark.sql import functions as F

# SCD2 current rows
df_customer = spark.table("silver_customer_scd2").filter(F.col("is_current") == 1)
df_account  = spark.table("silver_account_scd2").filter(F.col("is_current") == 1)
df_loan     = spark.table("silver_loan_scd2").filter(F.col("is_current") == 1)

# Standard silver tables
df_branch    = spark.table("silver_branches")
df_card      = spark.table("silver_cards")
df_channel   = spark.table("silver_channels")
df_txn_type  = spark.table("silver_txn_types")
df_txn       = spark.table("silver_transactions")
df_bal       = spark.table("silver_balances_daily")
df_loan_pay  = spark.table("silver_loan_payments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DIM_DATE**

# CELL ********************

df_dates = (
    df_txn.select(F.to_date("transaction_ts").alias("Date"))
    .union(df_bal.select(F.to_date("snapshot_date").alias("Date")))
    .union(df_loan.select(F.to_date("start_date").alias("Date")))
    .union(df_loan_pay.select(F.to_date("payment_date").alias("Date")))
    .filter(F.col("Date").isNotNull())
    .distinct()
)

dim_date = (
    df_dates
    .withColumn("date_key", F.date_format("Date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("Date"))
    .withColumn("month_no", F.month("Date"))
    .withColumn("month_name", F.date_format("Date", "MMM"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("Date")))
    .withColumn("day_of_month", F.dayofmonth("Date"))
    .withColumn("day_name", F.date_format("Date", "E"))
    .withColumn("week_of_year", F.weekofyear("Date"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType
from functools import reduce
from datetime import timedelta

namespace = "bank_silver_lakehouse"

# Only use business/reporting tables to derive calendar range
tables_to_scan = [
    f"{namespace}.fact_transactions",
    f"{namespace}.fact_daily_balances",
    f"{namespace}.fact_loans",
    f"{namespace}.fact_loan_payments",
    f"{namespace}.fact_branch_daily_summary",
    f"{namespace}.fact_customer_daily_portfolio",
    f"{namespace}.silver_transactions",
    f"{namespace}.silver_balances_daily",
    f"{namespace}.silver_loans",
    f"{namespace}.silver_loan_payments",
    f"{namespace}.silver_accounts",
    f"{namespace}.silver_cards",
    f"{namespace}.silver_branches"
]

# Exclude non-business dates that should not drive the calendar range
exclude_exact = {
    "dob", "date_of_birth", "birth_date",
    "effective_start_date", "effective_end_date",
    "valid_from", "valid_to",
    "created_date", "updated_date",
    "created_ts", "updated_ts",
    "load_date", "load_ts",
    "ingest_date", "ingest_ts"
}

exclude_contains = [
    "dob", "birth", "effective", "valid",
    "created", "updated", "modified",
    "load", "ingest", "audit"
]

def usable_date_col(col_name: str) -> bool:
    c = col_name.lower()
    if c in exclude_exact:
        return False
    if any(x in c for x in exclude_contains):
        return False
    return True

date_range_dfs = []

for tbl in tables_to_scan:
    if not spark.catalog.tableExists(tbl):
        print(f"Skipping missing table: {tbl}")
        continue

    df = spark.table(tbl)

    date_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, (DateType, TimestampType)) and usable_date_col(field.name)
    ]

    print(f"{tbl} -> date columns used: {date_cols}")

    for col_name in date_cols:
        date_range_dfs.append(
            df.select(
                F.lit(tbl).alias("table_name"),
                F.lit(col_name).alias("column_name"),
                F.min(F.to_date(F.col(col_name))).alias("min_date"),
                F.max(F.to_date(F.col(col_name))).alias("max_date")
            )
        )

if not date_range_dfs:
    raise Exception("No usable date/timestamp columns found in the selected banking tables.")

date_bounds_by_column = reduce(lambda d1, d2: d1.unionByName(d2), date_range_dfs) \
    .filter(F.col("min_date").isNotNull() & F.col("max_date").isNotNull())

display(date_bounds_by_column.orderBy("table_name", "column_name"))

calendar_bounds = (
    date_bounds_by_column
    .agg(
        F.min("min_date").alias("final_min_date"),
        F.max("max_date").alias("final_max_date")
    )
    .collect()[0]
)

min_date = calendar_bounds["final_min_date"]
max_date = calendar_bounds["final_max_date"]

if min_date is None or max_date is None:
    raise Exception("Could not derive min/max date from the selected tables.")

# Optional future buffer for planning/reporting
max_date_with_buffer = max_date + timedelta(days=365)

min_date_str = min_date.strftime("%Y-%m-%d")
max_date_str = max_date_with_buffer.strftime("%Y-%m-%d")

print(f"Derived Min Date: {min_date_str}")
print(f"Derived Max Date: {max_date_str}")

dim_date = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{min_date_str}'),
                to_date('{max_date_str}'),
                interval 1 day
            )
        ) AS Date
    """)
    .withColumn("Date_Key", F.date_format("Date", "yyyyMMdd").cast("int"))
    .withColumn("Year", F.year("Date"))
    .withColumn("Month_Number", F.month("Date"))
    .withColumn("Month_Name", F.date_format("Date", "MMMM"))
    .withColumn("Month_Short", F.date_format("Date", "MMM"))
    .withColumn("Quarter_Number", F.quarter("Date"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("Date")))   # keep lowercase to avoid existing conflict
    .withColumn("Year_Quarter", F.concat(F.year("Date"), F.lit("-Q"), F.quarter("Date")))
    .withColumn("Month_Year", F.date_format("Date", "MMM yyyy"))
    .withColumn("Year_Month", F.date_format("Date", "yyyy-MM"))
    .withColumn("Week_Number", F.weekofyear("Date"))
    .withColumn("Day", F.dayofmonth("Date"))
    .withColumn("Day_Name", F.date_format("Date", "EEEE"))
    .withColumn("Day_Short", F.date_format("Date", "E"))
    .withColumn("Day_Of_Week", F.dayofweek("Date"))
    .withColumn("Day_Of_Year", F.dayofyear("Date"))
    .withColumn("Is_Weekend", F.when(F.dayofweek("Date").isin(1, 7), 1).otherwise(0))
    .withColumn("Is_Month_Start", F.when(F.dayofmonth("Date") == 1, 1).otherwise(0))
    .withColumn("Is_Month_End", F.when(F.last_day("Date") == F.col("Date"), 1).otherwise(0))
    .withColumn(
        "Is_Quarter_Start",
        F.when(
            (F.dayofmonth("Date") == 1) & (F.month("Date").isin(1, 4, 7, 10)),
            1
        ).otherwise(0)
    )
    .withColumn(
        "Is_Quarter_End",
        F.when(
            (F.last_day("Date") == F.col("Date")) & (F.month("Date").isin(3, 6, 9, 12)),
            1
        ).otherwise(0)
    )
    .withColumn("Is_Year_Start", F.when((F.month("Date") == 1) & (F.dayofmonth("Date") == 1), 1).otherwise(0))
    .withColumn("Is_Year_End", F.when((F.month("Date") == 12) & (F.dayofmonth("Date") == 31), 1).otherwise(0))
    .withColumn(
        "Financial_Year",
        F.when(
            F.month("Date") >= 4,
            F.concat(F.year("Date").cast("string"), F.lit("-"), (F.year("Date") + 1).cast("string"))
        ).otherwise(
            F.concat((F.year("Date") - 1).cast("string"), F.lit("-"), F.year("Date").cast("string"))
        )
    )
    .withColumn(
        "Financial_Quarter",
        F.when(F.month("Date").isin(4, 5, 6), F.lit("Q1"))
         .when(F.month("Date").isin(7, 8, 9), F.lit("Q2"))
         .when(F.month("Date").isin(10, 11, 12), F.lit("Q3"))
         .otherwise(F.lit("Q4"))
    )
)

display(dim_date)

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{namespace}.dim_date")

spark.sql(f"""
SELECT 
    MIN(Date) AS MinDate,
    MAX(Date) AS MaxDate,
    COUNT(*) AS RowCount
FROM {namespace}.dim_date
""").show()

spark.table(f"{namespace}.dim_date").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date.write.mode("overwrite").format("delta").saveAsTable("bank_silver_lakehouse.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT 
    MIN(Date) AS MinDate,
    MAX(Date) AS MaxDate,
    COUNT(*) AS TotalRows
FROM bank_silver_lakehouse.dim_date
""").show()

spark.sql("""
SELECT *
FROM bank_silver_lakehouse.dim_date
ORDER BY Date
LIMIT 10
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DIM_BRANCH**

# CELL ********************

dim_branch = (
    df_branch
    .select(
        F.monotonically_increasing_id().alias("branch_key"),
        "branch_id",
        "branch_name",
        "city",
        "state",
        "region",
        "opened_date",
        "is_active"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DIM_CUSTOMER**

# CELL ********************

dim_customer = (
    df_customer
    .select(
        F.monotonically_increasing_id().alias("customer_key"),
        "customer_id",
        "full_name",
        "dob",
        "gender",
        "pan",
        "phone",
        "email",
        "address",
        "city",
        "state",
        "segment",
        "risk_rating",
        "home_branch_id",
        "kyc_status",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DIM_ACCOUNT**

# CELL ********************

dim_account = (
    df_account
    .select(
        F.monotonically_increasing_id().alias("account_key"),
        "account_id",
        "customer_id",
        "branch_id",
        "account_type",
        "currency",
        "status",
        "open_date",
        "overdraft_limit",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DIM_CARD**

# CELL ********************

dim_card = (
    df_card
    .select(
        F.monotonically_increasing_id().alias("card_key"),
        "card_id",
        "customer_id",
        "linked_account_id",
        "card_type",
        "status",
        "issued_date",
        "credit_limit"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DIM_LOAN**

# CELL ********************

from pyspark.sql import functions as F

df_loan = spark.table("bank_silver_lakehouse.silver_loan_scd2")

df_loan_current = df_loan.filter(F.col("is_current") == 1)

dim_loan = (
    df_loan_current
    .select(
        F.monotonically_increasing_id().alias("loan_key"),
        "loan_id",
        "customer_id",
        "branch_id",
        "status",
        "start_date",
        "interest_rate_apr",
        "principal_amount",
        "tenure_months",
        "effective_from",
        "effective_to",
        "is_current"
    )
)

dim_loan.groupBy("loan_id").count().filter("count > 1").show()

display(dim_loan)
# dim_loan = (
#     df_loan
#     .filter(F.col("is_current") == 1)
#     .select(
#         F.monotonically_increasing_id().alias("loan_key"),
#         "loan_id",
#         "customer_id",
#         "branch_id",
#         "loan_type",
#         "status",
#         "start_date",
#         "interest_rate_apr",
#         "principal_amount",
#         "tenure_months",
#         "effective_from",
#         "effective_to",
#         "is_current"
#     )
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DIM_CHANNEL**

# CELL ********************

dim_channel = (
    df_channel
    .select(
        F.monotonically_increasing_id().alias("channel_key"),
        F.col("channel_name")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DIM_TXN_TYPE**

# CELL ********************

dim_txn_type = (
    df_txn_type
    .select(
        F.monotonically_increasing_id().alias("txn_type_key"),
        F.col("txn_type_name")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Dimensions for Facts**

# CELL ********************

dim_date = spark.table("dim_date")
dim_customer = spark.table("dim_customer")
dim_account = spark.table("dim_account")
dim_branch = spark.table("dim_branch")
dim_loan = spark.table("dim_loan")
dim_channel = spark.table("dim_channel")
dim_txn_type = spark.table("dim_txn_type")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fact_Transaction**

# CELL ********************

df_txn.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_transactions = (
#     df_txn.alias("t")
#     .join(dim_account.select("account_key", "account_id").alias("a"),
#           F.col("t.account_id") == F.col("a.account_id"), "left")
#     .join(dim_customer.select("customer_key", "customer_id").alias("c"),
#           F.col("t.customer_id") == F.col("c.customer_id"), "left")
#     .join(dim_branch.select("branch_key", "branch_id").alias("b"),
#           F.col("t.branch_id") == F.col("b.branch_id"), "left")
#     .join(dim_channel.select("channel_key", F.col("channel_name").alias("join_channel")).alias("ch"),
#           F.col("t.channel") == F.col("ch.join_channel"), "left")
#     .join(dim_txn_type.select("txn_type_key", F.col("txn_type_name").alias("join_txn_type")).alias("tt"),
#           F.col("t.txn_type") == F.col("tt.join_txn_type"), "left")
#     .join(dim_date.select("date_key", "Date").alias("d"),
#           F.to_date(F.col("t.transaction_ts")) == F.col("d.Date"), "left")
#     .select(
#         F.col("a.account_key"),
#         F.col("b.branch_key"),
#         F.col("ch.channel_key"),
#         F.col("t.counterparty"),
#         F.col("t.currency"),
#         F.col("c.customer_key"),
#         F.col("d.date_key"),
#         F.col("t.direction"),
#         F.col("t.is_fraud_label"),
#         F.col("t.merchant_category"),
#         F.col("t.transaction_id"),
#         F.col("t.transaction_ts"),
#         F.col("tt.txn_type_key"),
#         F.col("t.amount")
#     )
# )


fact_transactions = (
    df_txn.alias("t")
    .join(
        dim_account.select("account_key", "account_id").alias("a"),
        F.col("t.account_id") == F.col("a.account_id"),
        "left"
    )
    .join(
        dim_customer.select("customer_key", "customer_id").alias("c"),
        F.col("t.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        dim_branch.select("branch_key", "branch_id").alias("b"),
        F.col("t.branch_id") == F.col("b.branch_id"),
        "left"
    )
    .join(
        dim_channel.select("channel_key", F.col("channel_name").alias("join_channel")).alias("ch"),
        F.col("t.channel") == F.col("ch.join_channel"),
        "left"
    )
    .join(
        dim_txn_type.select("txn_type_key", F.col("txn_type_name").alias("txn_type")).alias("tt"),
        F.col("t.txn_type") == F.col("tt.txn_type"),
        "left"
    )
    .join(
        dim_date.select("date_key", "Date").alias("d"),
        F.to_date(F.col("t.transaction_ts")) == F.col("d.Date"),
        "left"
    )
    .select(
        F.col("a.account_key"),
        F.col("b.branch_key"),
        F.col("ch.channel_key"),
        F.col("t.counterparty"),
        F.col("t.currency"),
        F.col("c.customer_key"),
        F.col("d.date_key"),
        F.col("t.direction"),
        F.col("t.is_fraud_label"),
        F.col("t.merchant_category"),
        F.col("t.transaction_id"),
        F.col("t.transaction_ts"),
        F.col("tt.txn_type_key"),
        F.col("t.amount")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fact Daily Balances**

# CELL ********************

fact_daily_balances = (
    df_bal.alias("bd")
    .join(dim_account.select("account_key", "account_id").alias("a"),
          F.col("bd.account_id") == F.col("a.account_id"), "left")
    .join(dim_customer.select("customer_key", "customer_id").alias("c"),
          F.col("bd.customer_id") == F.col("c.customer_id"), "left")
    .join(dim_branch.select("branch_key", "branch_id").alias("b"),
          F.col("bd.branch_id") == F.col("b.branch_id"), "left")
    .join(dim_date.select("date_key", "Date").alias("d"),
          F.to_date(F.col("bd.snapshot_date")) == F.col("d.Date"), "left")
    .select(
        F.col("a.account_key"),
        F.col("b.branch_key"),
        F.col("bd.currency"),
        F.col("c.customer_key"),
        F.col("d.date_key"),
        F.col("bd.end_of_day_balance")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fact Loans**

# CELL ********************

loan_src = (
    df_loan
    .withColumn("start_date", F.to_date(F.col("start_date")))
    .withColumn("principal_amount", F.col("principal_amount").cast("decimal(18,2)"))
    .withColumn("interest_rate_apr", F.col("interest_rate_apr").cast("decimal(10,4)"))
    .withColumn("tenure_months", F.col("tenure_months").cast("int"))
    .alias("l")
)

loan_src = (
    df_loan_current
    .withColumn("start_date", F.to_date(F.col("start_date")))
    .withColumn("principal_amount", F.col("principal_amount").cast("decimal(18,2)"))
    .withColumn("interest_rate_apr", F.col("interest_rate_apr").cast("decimal(10,4)"))
    .withColumn("tenure_months", F.col("tenure_months").cast("int"))
    .alias("l")
)

fact_loans = (
    loan_src
    .join(
        dim_customer.select("customer_key", "customer_id").alias("c"),
        F.col("l.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        dim_branch.select("branch_key", "branch_id").alias("b"),
        F.col("l.branch_id") == F.col("b.branch_id"),
        "left"
    )
    .join(
        dim_loan.select("loan_key", "loan_id").alias("dl"),
        F.col("l.loan_id") == F.col("dl.loan_id"),
        "left"
    )
    .join(
        dim_date.select("date_key", "Date").alias("d"),
        F.col("l.start_date") == F.col("d.Date"),
        "left"
    )
    .select(
        F.col("dl.loan_key"),
        F.col("d.date_key"),
        F.col("c.customer_key"),
        F.col("b.branch_key"),
        F.col("l.principal_amount"),
        F.col("l.interest_rate_apr"),
        F.col("l.tenure_months"),
        F.col("l.status")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fact Loan Payments**

# CELL ********************

from pyspark.sql import functions as F

# Load payment source table
df_loan_pay = spark.table("bank_silver_lakehouse.silver_loan_payments")

# Summary
df_loan_pay.select(
    F.min(F.to_date("payment_date")).alias("min_payment_date"),
    F.max(F.to_date("payment_date")).alias("max_payment_date"),
    F.count("*").alias("row_cnt"),
    F.countDistinct(F.to_date("payment_date")).alias("distinct_payment_dates"),
    F.sum(F.when(F.to_date("payment_date").isNull(), 1).otherwise(0)).alias("null_payment_dates")
).show()

# Distribution by date
df_loan_pay.groupBy(F.to_date("payment_date").alias("payment_dt")) \
    .agg(
        F.count("*").alias("rows"),
        F.sum("payment_amount").alias("total_payment")
    ) \
    .orderBy("payment_dt") \
    .show(100, False)

fact_loan_payments = (
    df_loan_pay.alias("lp")
    .join(dim_branch.select("branch_key", "branch_id").alias("b"),
          F.col("lp.branch_id") == F.col("b.branch_id"), "left")
    .join(dim_customer.select("customer_key", "customer_id").alias("c"),
          F.col("lp.customer_id") == F.col("c.customer_id"), "left")
    .join(dim_loan.select("loan_key", "loan_id").alias("dl"),
          F.col("lp.loan_id") == F.col("dl.loan_id"), "left")
    .join(dim_date.select("date_key", "Date").alias("d"),
          F.to_date(F.col("lp.payment_date")) == F.col("d.Date"), "left")
    .select(
        F.col("b.branch_key"),
        F.col("c.customer_key"),
        F.col("d.date_key"),
        F.col("dl.loan_key"),
        F.col("lp.payment_amount"),
        F.col("lp.payment_id"),
        F.col("lp.status")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fact Branch Daily Summary**

# CELL ********************

from pyspark.sql import functions as F

# Read source tables directly
df_bal_daily = spark.table("bank_silver_lakehouse.silver_balances_daily")
dim_date = spark.table("bank_silver_lakehouse.dim_date")
dim_account = spark.table("bank_silver_lakehouse.dim_account")
dim_customer = spark.table("bank_silver_lakehouse.dim_customer")
dim_branch = spark.table("bank_silver_lakehouse.dim_branch")

# Create fact_daily_balances
fact_daily_balances = (
    df_bal_daily.alias("bd")
    .join(
        dim_account.select("account_key", "account_id").alias("a"),
        on="account_id",
        how="left"
    )
    .join(
        dim_customer.select("customer_key", "customer_id").alias("c"),
        on="customer_id",
        how="left"
    )
    .join(
        dim_branch.select("branch_key", "branch_id").alias("b"),
        on="branch_id",
        how="left"
    )
    .join(
        dim_date.select(
            F.col("Date_Key").alias("date_key"),
            F.col("Date").alias("join_date")
        ).alias("d"),
        F.to_date(F.col("bd.snapshot_date")) == F.col("d.join_date"),
        how="left"
    )
    .select(
        F.col("a.account_key"),
        F.col("c.customer_key"),
        F.col("b.branch_key"),
        F.col("d.date_key"),
        F.col("bd.snapshot_date"),
        F.col("bd.end_of_day_balance"),
        F.col("bd.currency")
    )
)

display(fact_daily_balances)

fact_daily_balances.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_silver_lakehouse.fact_daily_balances")

# Create fact_branch_daily_summary
fact_branch_daily_summary = (
    fact_daily_balances
    .groupBy("branch_key", "date_key")
    .agg(
        F.sum("end_of_day_balance").alias("total_balance"),
        F.countDistinct("account_key").alias("total_accounts"),
        F.countDistinct("customer_key").alias("total_customers")
    )
)

display(fact_branch_daily_summary)

fact_branch_daily_summary.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_silver_lakehouse.fact_branch_daily_summary")

# txn_branch = (
#     df_fact_transactions.groupBy("date_key", "branch_key")
#     .agg(
#         F.count("*").alias("transaction_count"),
#         F.sum("amount").alias("transaction_amount"),
#         F.sum(F.when(F.col("is_fraud_label") == 1, 1).otherwise(0)).alias("fraud_txn_count")
#     )
# )

# bal_branch = (
#     df_fact_balances.groupBy("date_key", "branch_key")
#     .agg(F.sum("end_of_day_balance").alias("deposit_balance"))
# )

# loan_branch = (
#     df_fact_loans.groupBy("date_key", "branch_key")
#     .agg(
#         F.count("*").alias("loan_count"),
#         F.sum("principal_amount").alias("loan_principal")
#     )
# )

# pay_branch = (
#     df_fact_loan_payments.groupBy("date_key", "branch_key")
#     .agg(F.sum("payment_amount").alias("payment_amount"))
# )

# df_branch_daily = (
#     txn_branch.alias("t")
#     .join(bal_branch.alias("b"), ["date_key", "branch_key"], "full")
#     .join(loan_branch.alias("l"), ["date_key", "branch_key"], "full")
#     .join(pay_branch.alias("p"), ["date_key", "branch_key"], "full")
#     .na.fill(0)
# )

# df_branch_daily.write.mode("overwrite").format("delta").saveAsTable("fact_branch_daily_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fact Customer Daily Portfolio**

# CELL ********************

from pyspark.sql import functions as F

df_loans = spark.table("bank_silver_lakehouse.silver_loan_scd2")
dim_date = spark.table("bank_silver_lakehouse.dim_date")
dim_customer = spark.table("bank_silver_lakehouse.dim_customer")
dim_branch = spark.table("bank_silver_lakehouse.dim_branch")

customer_loan_base = (
    df_loans.alias("l")
    .filter(F.col("is_current") == 1)
    .join(
        dim_customer.select("customer_key", "customer_id").alias("c"),
        on="customer_id",
        how="left"
    )
    .join(
        dim_branch.select("branch_key", "branch_id").alias("b"),
        on="branch_id",
        how="left"
    )
    .join(
        dim_date.select(
            F.col("Date_Key").alias("date_key"),
            F.col("Date").alias("join_date")
        ).alias("d"),
        F.to_date(F.col("l.start_date")) == F.col("d.join_date"),
        how="left"
    )
    .select(
        F.col("c.customer_key"),
        F.col("b.branch_key"),
        F.col("d.date_key"),
        F.col("l.loan_id"),
        F.col("l.principal_amount"),
        F.col("l.interest_rate_apr"),
        F.col("l.tenure_months")
    )
)

fact_customer_daily_portfolio = (
    customer_loan_base
    .groupBy("customer_key", "date_key")
    .agg(
        F.countDistinct("loan_id").alias("loan_count"),
        F.sum("principal_amount").alias("total_principal_amount"),
        F.avg("interest_rate_apr").alias("avg_interest_rate_apr"),
        F.avg("tenure_months").alias("avg_tenure_months")
    )
)

display(fact_customer_daily_portfolio)

fact_customer_daily_portfolio.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_silver_lakehouse.fact_customer_daily_portfolio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_transactions.select(
    F.count("*").alias("rows"),
    F.count("date_key").alias("date_key_ok"),
    F.count("customer_key").alias("customer_key_ok"),
    F.count("account_key").alias("account_key_ok"),
    F.count("branch_key").alias("branch_key_ok"),
    F.count("channel_key").alias("channel_key_ok"),
    F.count("txn_type_key").alias("txn_type_key_ok")
).show()

fact_daily_balances.select(
    F.count("*").alias("rows"),
    F.count("date_key").alias("date_key_ok"),
    F.count("customer_key").alias("customer_key_ok"),
    F.count("account_key").alias("account_key_ok"),
    F.count("branch_key").alias("branch_key_ok")
).show()

fact_loans.select(
    F.count("*").alias("rows"),
    F.count("date_key").alias("date_key_ok"),
    F.count("customer_key").alias("customer_key_ok"),
    F.count("branch_key").alias("branch_key_ok"),
    F.count("loan_key").alias("loan_key_ok")
).show()

fact_loan_payments.select(
    F.count("*").alias("rows"),
    F.count("date_key").alias("date_key_ok"),
    F.count("customer_key").alias("customer_key_ok"),
    F.count("branch_key").alias("branch_key_ok"),
    F.count("loan_key").alias("loan_key_ok")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Dimensions
# dim_date.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_date")
# dim_customer.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_customer")
# dim_account.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_account")
# dim_branch.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_branch")
# dim_card.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_card")
# dim_loan.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_loan")
# dim_channel.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_channel")
# dim_txn_type.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("dim_txn_type")

# # Facts
# fact_transactions.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("fact_transactions")
# fact_daily_balances.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("fact_daily_balances")
# fact_loans.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("fact_loans")
# fact_loan_payments.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("fact_loan_payments")
# fact_customer_daily_portfolio.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("fact_customer_daily_portfolio")
# fact_branch_daily_summary.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("fact_branch_daily_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customer = spark.table("silver_customer_scd2").filter(F.col("is_current") == 1)
df_account = spark.table("silver_account_scd2").filter(F.col("is_current") == 1)
df_loan = spark.table("silver_loan_scd2").filter(F.col("is_current") == 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT COUNT(*) FROM dim_customer").show()
spark.sql("SELECT COUNT(*) FROM dim_account").show()
spark.sql("SELECT COUNT(*) FROM dim_loan").show()

spark.sql("SELECT COUNT(*) FROM fact_transactions").show()
spark.sql("SELECT COUNT(*) FROM fact_daily_balances").show()
spark.sql("SELECT COUNT(*) FROM fact_loans").show()
spark.sql("SELECT COUNT(*) FROM fact_loan_payments").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

workspace_id = "39d0b856-8252-47ff-a893-808b73f74d8c"
warehouse_id = "43370c1f-fa94-4fb1-981a-cb1c949d7c0d"
warehouse_name = "Bank_Gold_Warehouse"
sql_endpoint = "bzhc5bdddrdudnn4fof25uxzta-k24naoksql7upketqcfxh52nrq.datawarehouse.fabric.microsoft.com,1433"

# Explicitly set the warehouse SQL endpoint
spark.conf.set(f"spark.datawarehouse.{warehouse_name}.sqlendpoint", sql_endpoint)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

workspace_id = "39d0b856-8252-47ff-a893-808b73f74d8c"
warehouse_id = "43370c1f-fa94-4fb1-981a-cb1c949d7c0d"

dim_date_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.dim_date")
)
display(dim_date_df)

dim_customer_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.dim_customer")
)
display(dim_customer_df)

dim_account_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.dim_account")
)
display(dim_account_df)

dim_branch_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.dim_branch")
)
display(dim_branch_df)

dim_card_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.dim_card")
)
display(dim_card_df)

dim_loan_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.dim_loan")
)
display(dim_loan_df)

dim_channel_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.dim_channel")
)
display(dim_channel_df)

dim_txn_type_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.dim_txn_type")
)
display(dim_txn_type_df)

fact_transactions_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.fact_transactions")
)
display(fact_transactions_df)

fact_daily_balances_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.fact_daily_balances")
)
display(fact_daily_balances_df)

fact_loans_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.fact_loans")
)
display(fact_loans_df)

fact_loan_payments_df = (
    spark.read
         .option(Constants.WorkspaceId, workspace_id)
         .option(Constants.DatawarehouseId, warehouse_id)
         .synapsesql("Bank_Gold_Warehouse.dbo.fact_loan_payments")
)
display(fact_loan_payments_df)

# fact_customer_daily_portfolio_df = (
#     spark.read
#          .option(Constants.WorkspaceId, workspace_id)
#          .option(Constants.DatawarehouseId, warehouse_id)
#          .synapsesql("Bank_Gold_Warehouse.dbo.fact_customer_daily_portfolio")
# )
# display(fact_customer_daily_portfolio_df)

# fact_branch_daily_summary_df = (
#     spark.read
#          .option(Constants.WorkspaceId, workspace_id)
#          .option(Constants.DatawarehouseId, warehouse_id)
#          .synapsesql("Bank_Gold_Warehouse.dbo.fact_branch_daily_summary")
# )
# display(fact_branch_daily_summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_card.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_card")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_date")
dim_branch.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_branch")
dim_customer.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_customer")
dim_account.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_account")
dim_card.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_card")
dim_loan.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_loan")
dim_channel.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_channel")
dim_txn_type.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.dim_txn_type")

fact_transactions.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.fact_transactions")
fact_daily_balances.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.fact_daily_balances")
fact_loans.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.fact_loans")
fact_loan_payments.write.mode("overwrite").synapsesql("Bank_Gold_Warehouse.dbo.fact_loan_payments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

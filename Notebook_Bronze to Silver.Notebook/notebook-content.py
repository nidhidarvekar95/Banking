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
# META           "id": "ea9c98c3-5f43-4cf8-9824-5c6627d20208"
# META         },
# META         {
# META           "id": "b87fc055-5393-41e8-8bef-b8a08520a62f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

from pyspark.sql import functions as F, Window

bronze_transactions_path = "abfss://Banking_DEV@onelake.dfs.fabric.microsoft.com/Bank_Bronze_Lakehouse.Lakehouse/Tables/dbo.accounts"

df = spark.read.format("delta").load(bronze_transactions_path)
df.display()

df = spark.sql("SELECT * FROM dbo.accounts")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.accounts`")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window

df_accounts = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.accounts`")
display(df_accounts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_accounts = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.accounts`")

silver_accounts = (
    df_accounts
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("account_type", F.upper(F.trim(F.col("account_type"))))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("open_date", F.to_date(F.col("open_date")))
    .withColumn("overdraft_limit", F.col("overdraft_limit").cast("decimal(18,2)"))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
    .withColumn("record_source", F.trim(F.col("record_source")))
    .withColumn("silver_load_ts", F.current_timestamp())
)

display(silver_accounts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_accounts.write.mode("overwrite").format("delta").saveAsTable("silver_accounts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Helper functions**

# CELL ********************

from pyspark.sql import functions as F, Window

def rename_if_exists(df, old_name, new_name):
    if old_name in df.columns and new_name not in df.columns:
        return df.withColumnRenamed(old_name, new_name)
    return df

def trim_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df

def upper_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.upper(F.trim(F.col(c))))
    return df

def lower_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.lower(F.trim(F.col(c))))
    return df

def date_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_date(F.col(c)))
    return df

def ts_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df

def decimal_cols(df, cols, precision=18, scale=2):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.regexp_replace(F.col(c).cast("string"), ",", "").cast(f"decimal({precision},{scale})")
            )
    return df

def int_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("int"))
    return df

def keep_non_blank_keys(df, key_cols):
    for c in key_cols:
        if c in df.columns:
            df = df.filter(F.col(c).isNotNull() & (F.trim(F.col(c).cast("string")) != ""))
    return df

def dedupe_latest(df, key_col, order_cols=None):
    if key_col not in df.columns:
        return df
    order_cols = [c for c in (order_cols or []) if c in df.columns]
    if not order_cols:
        order_cols = ["silver_load_ts"]
    w = Window.partitionBy(key_col).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

def add_audit_cols(df):
    if "record_source" in df.columns:
        df = df.withColumn("record_source", F.trim(F.col("record_source")))
    else:
        df = df.withColumn("record_source", F.lit("bronze"))
    df = df.withColumn("silver_load_ts", F.current_timestamp())
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Customer Transformation**

# CELL ********************

df_customers = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.customers`")

# Common renames if your source uses alternate names
df_customers = rename_if_exists(df_customers, "date_of_birth", "dob")
df_customers = rename_if_exists(df_customers, "phone_number", "phone")
df_customers = rename_if_exists(df_customers, "mobile_no", "phone")
df_customers = rename_if_exists(df_customers, "customer_status", "status")

silver_customers = df_customers

silver_customers = trim_cols(
    silver_customers,
    [
        "customer_id", "first_name", "last_name", "full_name", "gender",
        "email", "phone", "city", "state", "country", "status", "record_source"
    ]
)

silver_customers = upper_cols(
    silver_customers,
    ["gender", "city", "state", "country", "status"]
)

silver_customers = lower_cols(
    silver_customers,
    ["email"]
)

silver_customers = date_cols(
    silver_customers,
    ["dob"]
)

silver_customers = ts_cols(
    silver_customers,
    ["created_at", "updated_at"]
)

if "phone" in silver_customers.columns:
    silver_customers = silver_customers.withColumn(
        "phone",
        F.regexp_replace(F.col("phone"), r"[^0-9]", "")
    )

if "dob" in silver_customers.columns:
    silver_customers = silver_customers.withColumn(
        "age_years",
        F.floor(F.months_between(F.current_date(), F.col("dob")) / 12)
    )

    silver_customers = silver_customers.withColumn(
        "age_band",
        F.when(F.col("age_years") < 18, "UNDER_18")
         .when((F.col("age_years") >= 18) & (F.col("age_years") <= 25), "18_25")
         .when((F.col("age_years") >= 26) & (F.col("age_years") <= 40), "26_40")
         .when((F.col("age_years") >= 41) & (F.col("age_years") <= 60), "41_60")
         .otherwise("60_PLUS")
    )

if "first_name" in silver_customers.columns and "last_name" in silver_customers.columns and "full_name" not in silver_customers.columns:
    silver_customers = silver_customers.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )

silver_customers = add_audit_cols(silver_customers)
silver_customers = keep_non_blank_keys(silver_customers, ["customer_id"])
silver_customers = dedupe_latest(silver_customers, "customer_id", ["updated_at", "created_at", "silver_load_ts"])

display(silver_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_cards.write.mode("overwrite").format("delta").saveAsTable("silver_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Branches Transformation**

# CELL ********************

df_branches = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.branches`")

df_branches = rename_if_exists(df_branches, "branch_status", "status")

silver_branches = df_branches

silver_branches = trim_cols(
    silver_branches,
    [
        "branch_id", "branch_code", "branch_name", "city", "state",
        "country", "status", "record_source"
    ]
)

silver_branches = upper_cols(
    silver_branches,
    ["branch_code", "city", "state", "country", "status"]
)

silver_branches = ts_cols(
    silver_branches,
    ["created_at", "updated_at"]
)

silver_branches = add_audit_cols(silver_branches)
silver_branches = keep_non_blank_keys(silver_branches, ["branch_id"])
silver_branches = dedupe_latest(silver_branches, "branch_id", ["updated_at", "created_at", "silver_load_ts"])

display(silver_branches)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_cards.write.mode("overwrite").format("delta").saveAsTable("silver_branches")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Transactions transformation**

# CELL ********************

df_transactions = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.transactions`")

df_transactions = rename_if_exists(df_transactions, "txn_id", "transaction_id")
df_transactions = rename_if_exists(df_transactions, "txn_date", "transaction_date")
df_transactions = rename_if_exists(df_transactions, "txn_type", "transaction_type")
df_transactions = rename_if_exists(df_transactions, "debit_credit_flag", "dr_cr_flag")
df_transactions = rename_if_exists(df_transactions, "transaction_ts", "transaction_timestamp")

silver_transactions = df_transactions

silver_transactions = trim_cols(
    silver_transactions,
    [
        "transaction_id", "account_id", "customer_id", "branch_id",
        "transaction_type", "dr_cr_flag", "currency", "description",
        "status", "channel", "record_source"
    ]
)

silver_transactions = upper_cols(
    silver_transactions,
    ["transaction_type", "dr_cr_flag", "currency", "status", "channel"]
)

silver_transactions = date_cols(
    silver_transactions,
    ["transaction_date"]
)

silver_transactions = ts_cols(
    silver_transactions,
    ["transaction_timestamp", "created_at", "updated_at"]
)

silver_transactions = decimal_cols(
    silver_transactions,
    ["amount", "transaction_amount", "fee_amount", "tax_amount", "balance_after_txn"]
)

# Standardize DR/CR values if present
if "dr_cr_flag" in silver_transactions.columns:
    silver_transactions = silver_transactions.withColumn(
        "dr_cr_flag",
        F.when(F.col("dr_cr_flag").isin("D", "DR", "DEBIT"), "DEBIT")
         .when(F.col("dr_cr_flag").isin("C", "CR", "CREDIT"), "CREDIT")
         .otherwise(F.col("dr_cr_flag"))
    )

# Standardize transaction type labels if present
if "transaction_type" in silver_transactions.columns:
    silver_transactions = silver_transactions.withColumn(
        "transaction_type",
        F.when(F.col("transaction_type").isin("ATM WDL", "ATM WITHDRAWAL"), "ATM_WITHDRAWAL")
         .when(F.col("transaction_type").isin("UPI", "UPI PAYMENT"), "UPI")
         .when(F.col("transaction_type").isin("NEFT"), "NEFT")
         .when(F.col("transaction_type").isin("IMPS"), "IMPS")
         .when(F.col("transaction_type").isin("RTGS"), "RTGS")
         .otherwise(F.col("transaction_type"))
    )

if "transaction_date" in silver_transactions.columns:
    silver_transactions = silver_transactions.withColumn("transaction_year", F.year("transaction_date"))
    silver_transactions = silver_transactions.withColumn("transaction_month", F.month("transaction_date"))

silver_transactions = add_audit_cols(silver_transactions)
silver_transactions = keep_non_blank_keys(silver_transactions, ["transaction_id", "account_id"])
silver_transactions = dedupe_latest(
    silver_transactions,
    "transaction_id",
    ["transaction_timestamp", "updated_at", "created_at", "silver_load_ts"]
)

display(silver_transactions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_cards.write.mode("overwrite").format("delta").saveAsTable("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# %% **Loans Transformation**

# CELL ********************

df_loans = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.loans`")

df_loans = rename_if_exists(df_loans, "loan_status", "status")
df_loans = rename_if_exists(df_loans, "disbursal_date", "disbursement_date")
df_loans = rename_if_exists(df_loans, "principal_amt", "principal_amount")
df_loans = rename_if_exists(df_loans, "loan_amt", "loan_amount")

silver_loans = df_loans

silver_loans = trim_cols(
    silver_loans,
    [
        "loan_id", "customer_id", "account_id", "branch_id", "loan_type",
        "status", "currency", "record_source"
    ]
)

silver_loans = upper_cols(
    silver_loans,
    ["loan_type", "status", "currency"]
)

silver_loans = date_cols(
    silver_loans,
    ["application_date", "approval_date", "disbursement_date", "start_date", "close_date", "maturity_date"]
)

silver_loans = ts_cols(
    silver_loans,
    ["created_at", "updated_at"]
)

silver_loans = decimal_cols(
    silver_loans,
    [
        "principal_amount", "loan_amount", "interest_rate", "emi_amount",
        "outstanding_amount", "overdue_amount", "processing_fee"
    ]
)

silver_loans = int_cols(
    silver_loans,
    ["tenure_months"]
)

silver_loans = add_audit_cols(silver_loans)
silver_loans = keep_non_blank_keys(silver_loans, ["loan_id", "customer_id"])
silver_loans = dedupe_latest(silver_loans, "loan_id", ["updated_at", "created_at", "silver_load_ts"])

display(silver_loans)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_cards.write.mode("overwrite").format("delta").saveAsTable("silver_loans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Cards Transformation**

# CELL ********************

df_cards = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.cards`")

df_cards = rename_if_exists(df_cards, "card_status", "status")

silver_cards = df_cards

silver_cards = trim_cols(
    silver_cards,
    [
        "card_id", "customer_id", "account_id", "card_number", "card_type",
        "network", "status", "currency", "record_source"
    ]
)

silver_cards = upper_cols(
    silver_cards,
    ["card_type", "network", "status", "currency"]
)

silver_cards = date_cols(
    silver_cards,
    ["issue_date", "expiry_date"]
)

silver_cards = ts_cols(
    silver_cards,
    ["created_at", "updated_at"]
)

silver_cards = decimal_cols(
    silver_cards,
    ["credit_limit", "available_limit", "cash_limit"]
)

# Optional masking if full card number exists
if "card_number" in silver_cards.columns:
    silver_cards = silver_cards.withColumn(
        "card_number_masked",
        F.concat(F.lit("XXXX-XXXX-XXXX-"), F.right(F.col("card_number"), 4))
    )

silver_cards = add_audit_cols(silver_cards)
silver_cards = keep_non_blank_keys(silver_cards, ["card_id", "customer_id"])
silver_cards = dedupe_latest(silver_cards, "card_id", ["updated_at", "created_at", "silver_load_ts"])

display(silver_cards)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_cards.write.mode("overwrite").format("delta").saveAsTable("silver_cards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window

df_customers = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.customers`")

w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last())

silver_customers = (
    df_customers
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

display(silver_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT customer_id, COUNT(*) cnt
FROM Bank_Bronze_Lakehouse.`dbo.customers`
GROUP BY customer_id
HAVING COUNT(*) > 1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Customer Table from Bronze**

# CELL ********************

from pyspark.sql import functions as F, Window

df_branches = spark.sql("""
SELECT *
FROM Bank_Bronze_Lakehouse.`dbo.customers`
""")

display(df_branches)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Clean & Transform**

# CELL ********************

spark.sql("DROP TABLE IF EXISTS dbo.silver_branches")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window

df_branches = spark.sql("""
SELECT *
FROM Bank_Bronze_Lakehouse.`dbo.branches`
""")

silver_branches_stage = (
    df_branches
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("branch_name", F.trim(F.col("branch_name")))
    .withColumn("city", F.upper(F.trim(F.col("city"))))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
    .withColumn("region", F.upper(F.trim(F.col("region"))))
    .withColumn("opened_date", F.to_date(F.col("opened_date")))
    .withColumn("is_active", F.col("is_active").cast("int"))
    .withColumn(
        "status",
        F.when(F.col("is_active") == 1, "ACTIVE").otherwise("INACTIVE")
    )
    .withColumn("record_source", F.lit("branches_bronze"))
    .withColumn("silver_load_ts", F.current_timestamp())
)

display(silver_branches_stage)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Reject Ops**

# CELL ********************

reject_branches = (
    silver_branches_stage
    .withColumn(
        "reject_reason",
        F.when(F.col("branch_id").isNull() | (F.col("branch_id") == ""), "MISSING_BRANCH_ID")
         .when(F.col("branch_name").isNull() | (F.trim(F.col("branch_name")) == ""), "MISSING_BRANCH_NAME")
         .otherwise(None)
    )
    .filter(F.col("reject_reason").isNotNull())
)

display(reject_branches)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Valid Rows**

# CELL ********************

valid_branches = (
    silver_branches_stage
    .filter(F.col("branch_id").isNotNull() & (F.col("branch_id") != ""))
    .filter(F.col("branch_name").isNotNull() & (F.trim(F.col("branch_name")) != ""))
)

display(valid_branches)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Deduplication**

# CELL ********************

w = Window.partitionBy("branch_id").orderBy(
    F.col("opened_date").desc_nulls_last(),
    F.col("silver_load_ts").desc_nulls_last()
)

silver_branches = (
    valid_branches
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

display(silver_branches)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Since old silver_branches was wrong, droping it first.**

# CELL ********************

spark.sql("DROP TABLE IF EXISTS dbo.silver_branches")
spark.sql("DROP TABLE IF EXISTS dbo.reject_branches")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Write Silver Branches**

# CELL ********************

silver_branches.write.mode("overwrite").format("delta").saveAsTable("silver_branches")
reject_branches.write.mode("overwrite").format("delta").saveAsTable("reject_branches")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS silver_branches")
spark.sql("DROP TABLE IF EXISTS reject_branches")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_branches.write.mode("overwrite").format("delta").saveAsTable("silver_branches")
reject_branches.write.mode("overwrite").format("delta").saveAsTable("reject_branches")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT COUNT(*) AS cnt FROM silver_branches").show()

spark.sql("""
SELECT branch_id, COUNT(*) AS cnt
FROM silver_branches
GROUP BY branch_id
HAVING COUNT(*) > 1
""").show()

spark.sql("SELECT * FROM silver_branches LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **One Time Code**

# CELL ********************

from pyspark.sql import functions as F, Window

def trim_if_exists(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df

def upper_if_exists(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.upper(F.trim(F.col(c))))
    return df

def lower_if_exists(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.lower(F.trim(F.col(c))))
    return df

def date_if_exists(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_date(F.col(c)))
    return df

def ts_if_exists(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df

def int_if_exists(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("int"))
    return df

def decimal_if_exists(df, cols, precision=18, scale=2):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.regexp_replace(F.col(c).cast("string"), ",", "").cast(f"decimal({precision},{scale})")
            )
    return df

def rename_if_exists(df, old_name, new_name):
    if old_name in df.columns and new_name not in df.columns:
        df = df.withColumnRenamed(old_name, new_name)
    return df

def table_exists(table_name):
    try:
        spark.table(table_name)
        return True
    except:
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Transform Customers**

# CELL ********************

df_customers = spark.sql("""
SELECT *
FROM Bank_Bronze_Lakehouse.`dbo.customers`
""")

print(df_customers.columns)
display(df_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_customers_stage = (
    df_customers
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("full_name", F.trim(F.col("full_name")))
    .withColumn("gender", F.upper(F.trim(F.col("gender"))))
    .withColumn("pan", F.upper(F.trim(F.col("pan"))))
    .withColumn("phone", F.regexp_replace(F.trim(F.col("phone")), r"[^0-9]", ""))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("address", F.trim(F.col("address")))
    .withColumn("city", F.upper(F.trim(F.col("city"))))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
    .withColumn("segment", F.upper(F.trim(F.col("segment"))))
    .withColumn("risk_rating", F.upper(F.trim(F.col("risk_rating"))))
    .withColumn("home_branch_id", F.trim(F.col("home_branch_id")))
    .withColumn("kyc_status", F.upper(F.trim(F.col("kyc_status"))))
    .withColumn("dob", F.to_date(F.col("dob")))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
    .withColumn("record_source", F.trim(F.col("record_source")))
    .withColumn("age_years", F.floor(F.months_between(F.current_date(), F.col("dob")) / 12))
    .withColumn(
        "age_band",
        F.when(F.col("age_years") < 18, "UNDER_18")
         .when(F.col("age_years") <= 25, "18_25")
         .when(F.col("age_years") <= 40, "26_40")
         .when(F.col("age_years") <= 60, "41_60")
         .otherwise("60_PLUS")
    )
    .withColumn("silver_load_ts", F.current_timestamp())
)

display(silver_customers_stage)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Reject Customers**

# CELL ********************

reject_customers = (
    silver_customers_stage
    .withColumn(
        "reject_reason",
        F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "MISSING_CUSTOMER_ID")
         .when(F.col("full_name").isNull() | (F.trim(F.col("full_name")) == ""), "MISSING_FULL_NAME")
         .when(F.col("dob").isNull(), "INVALID_DOB")
         .otherwise(None)
    )
    .filter(F.col("reject_reason").isNotNull())
)

display(reject_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Valid Customers**

# CELL ********************

valid_customers = (
    silver_customers_stage
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
    .filter(F.col("full_name").isNotNull() & (F.trim(F.col("full_name")) != ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Deduplicate Customers**

# CELL ********************

w = Window.partitionBy("customer_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("created_at").desc_nulls_last(),
    F.col("silver_load_ts").desc_nulls_last()
)

silver_customers = (
    valid_customers
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

display(silver_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Write SIlver Customer**

# CELL ********************

spark.sql("DROP TABLE IF EXISTS silver_customers")
spark.sql("DROP TABLE IF EXISTS reject_customers")

silver_customers.write.mode("overwrite").format("delta").saveAsTable("silver_customers")
reject_customers.write.mode("overwrite").format("delta").saveAsTable("reject_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %% **Validate** 

# CELL ********************

spark.sql("SELECT COUNT(*) AS cnt FROM silver_customers").show()

spark.sql("""
SELECT customer_id, COUNT(*) AS cnt
FROM silver_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
""").show()

spark.sql("SELECT * FROM silver_customers LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Bronze Account Table**

# CELL ********************

df_accounts = spark.sql("""
SELECT *
FROM Bank_Bronze_Lakehouse.`dbo.accounts`
""")

print(df_accounts.columns)
display(df_accounts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transform Accounts**

# CELL ********************

from pyspark.sql import functions as F, Window

df_accounts = spark.sql("""
SELECT *
FROM Bank_Bronze_Lakehouse.`dbo.accounts`
""")

silver_accounts_stage = (
    df_accounts
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("account_type", F.upper(F.trim(F.col("account_type"))))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("open_date", F.to_date(F.col("open_date")))
    .withColumn("overdraft_limit", F.col("overdraft_limit").cast("decimal(18,2)"))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
    .withColumn("record_source", F.trim(F.col("record_source")))
    .withColumn("account_age_days", F.datediff(F.current_date(), F.col("open_date")))
    .withColumn("account_open_year", F.year(F.col("open_date")))
    .withColumn(
        "is_overdraft_enabled",
        F.when(F.col("overdraft_limit") > 0, 1).otherwise(0)
    )
    .withColumn("silver_load_ts", F.current_timestamp())
)

display(silver_accounts_stage)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reject Accounts**

# CELL ********************

reject_accounts = (
    silver_accounts_stage
    .withColumn(
        "reject_reason",
        F.when(F.col("account_id").isNull() | (F.col("account_id") == ""), "MISSING_ACCOUNT_ID")
         .when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "MISSING_CUSTOMER_ID")
         .when(F.col("branch_id").isNull() | (F.col("branch_id") == ""), "MISSING_BRANCH_ID")
         .when(F.col("open_date").isNull(), "INVALID_OPEN_DATE")
         .otherwise(None)
    )
    .filter(F.col("reject_reason").isNotNull())
)

display(reject_accounts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Valid Accounts**

# CELL ********************

valid_accounts = (
    silver_accounts_stage
    .filter(F.col("account_id").isNotNull() & (F.col("account_id") != ""))
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
    .filter(F.col("branch_id").isNotNull() & (F.col("branch_id") != ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Deduplicate Accounts**

# CELL ********************

w = Window.partitionBy("account_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("created_at").desc_nulls_last(),
    F.col("silver_load_ts").desc_nulls_last()
)

silver_accounts = (
    valid_accounts
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

display(silver_accounts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation against Customers/Branches**

# CELL ********************

silver_accounts_validated = (
    silver_accounts.alias("a")
    .join(
        spark.table("silver_customers").select("customer_id").distinct().alias("c"),
        on="customer_id",
        how="left"
    )
    .join(
        spark.table("silver_branches").select("branch_id").distinct().alias("b"),
        on="branch_id",
        how="left"
    )
)

display(silver_accounts_validated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Accounts**

# CELL ********************

spark.sql("DROP TABLE IF EXISTS silver_accounts")
spark.sql("DROP TABLE IF EXISTS reject_accounts")

silver_accounts.write.mode("overwrite").format("delta").saveAsTable("silver_accounts")
reject_accounts.write.mode("overwrite").format("delta").saveAsTable("reject_accounts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Accounts**

# CELL ********************

spark.sql("SELECT COUNT(*) AS cnt FROM silver_accounts").show()

spark.sql("""
SELECT account_id, COUNT(*) AS cnt
FROM silver_accounts
GROUP BY account_id
HAVING COUNT(*) > 1
""").show()

spark.sql("SELECT * FROM silver_accounts LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Cards from Bronze**

# CELL ********************

df_cards = spark.sql("""
SELECT *
FROM Bank_Bronze_Lakehouse.`dbo.cards`
""")

print(df_cards.columns)
display(df_cards)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transform Cards**

# CELL ********************

silver_cards_stage = (
    df_cards
    .withColumn("card_id", F.trim(F.col("card_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("linked_account_id", F.trim(F.col("linked_account_id")))
    .withColumn("card_type", F.upper(F.trim(F.col("card_type"))))
    .withColumn("issued_date", F.to_date(F.col("issued_date")))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("credit_limit", F.col("credit_limit").cast("decimal(18,2)"))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
    .withColumn("record_source", F.trim(F.col("record_source")))
    .withColumn("days_since_issue", F.datediff(F.current_date(), F.col("issued_date")))
    .withColumn(
        "is_active_card",
        F.when(F.col("status").isin("ACTIVE", "OPEN"), 1).otherwise(0)
    )
    .withColumn("silver_load_ts", F.current_timestamp())
)

display(silver_cards_stage)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reject Cards**

# CELL ********************

reject_cards = (
    silver_cards_stage
    .withColumn(
        "reject_reason",
        F.when(F.col("card_id").isNull() | (F.col("card_id") == ""), "MISSING_CARD_ID")
         .when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "MISSING_CUSTOMER_ID")
         .when(F.col("linked_account_id").isNull() | (F.col("linked_account_id") == ""), "MISSING_LINKED_ACCOUNT_ID")
         .otherwise(None)
    )
    .filter(F.col("reject_reason").isNotNull())
)

display(reject_cards)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Valid Cards**

# CELL ********************

valid_cards = (
    silver_cards_stage
    .filter(F.col("card_id").isNotNull() & (F.col("card_id") != ""))
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Deduplicate Cards**

# CELL ********************

w = Window.partitionBy("card_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("created_at").desc_nulls_last(),
    F.col("silver_load_ts").desc_nulls_last()
)

silver_cards = (
    valid_cards
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

display(silver_cards)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation Joins**

# CELL ********************

silver_cards_validated = (
    silver_cards.alias("cd")
    .join(
        spark.table("silver_customers")
             .select("customer_id")
             .distinct()
             .alias("c"),
        on="customer_id",
        how="left"
    )
    .join(
        spark.table("silver_accounts")
             .select(F.col("account_id").alias("linked_account_id"))
             .distinct()
             .alias("a"),
        on="linked_account_id",
        how="left"
    )
)

display(silver_cards_validated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Cards**

# CELL ********************

spark.sql("DROP TABLE IF EXISTS silver_cards")
spark.sql("DROP TABLE IF EXISTS reject_cards")

silver_cards.write.mode("overwrite").format("delta").saveAsTable("silver_cards")
reject_cards.write.mode("overwrite").format("delta").saveAsTable("reject_cards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Cards**

# CELL ********************

spark.sql("SELECT COUNT(*) AS cnt FROM silver_cards").show()

spark.sql("""
SELECT card_id, COUNT(*) AS cnt
FROM silver_cards
GROUP BY card_id
HAVING COUNT(*) > 1
""").show()

spark.sql("SELECT * FROM silver_cards LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Transaction Table from Bronze**

# CELL ********************

df_transactions = spark.sql("""
SELECT *
FROM Bank_Bronze_Lakehouse.`dbo.transactions`
""")

print(df_transactions.columns)
display(df_transactions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Alternate Column Names**

# CELL ********************

df_transactions = rename_if_exists(df_transactions, "txn_id", "transaction_id")
df_transactions = rename_if_exists(df_transactions, "txn_date", "transaction_date")
df_transactions = rename_if_exists(df_transactions, "txn_timestamp", "transaction_timestamp")
df_transactions = rename_if_exists(df_transactions, "txn_type", "transaction_type")
df_transactions = rename_if_exists(df_transactions, "debit_credit_flag", "dr_cr_flag")
df_transactions = rename_if_exists(df_transactions, "account_number", "account_id")
df_transactions = rename_if_exists(df_transactions, "txn_amount", "amount")

print(df_transactions.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transform Transactions**

# CELL ********************

silver_transactions_stage = df_transactions

silver_transactions_stage = trim_if_exists(
    silver_transactions_stage,
    [
        "transaction_id", "account_id", "customer_id", "branch_id",
        "transaction_type", "dr_cr_flag", "currency", "status",
        "channel", "description", "record_source"
    ]
)

silver_transactions_stage = upper_if_exists(
    silver_transactions_stage,
    ["transaction_type", "dr_cr_flag", "currency", "status", "channel"]
)

silver_transactions_stage = date_if_exists(
    silver_transactions_stage,
    ["transaction_date"]
)

silver_transactions_stage = ts_if_exists(
    silver_transactions_stage,
    ["transaction_timestamp", "created_at", "updated_at"]
)

silver_transactions_stage = decimal_if_exists(
    silver_transactions_stage,
    ["amount", "fee_amount", "tax_amount", "balance_after_txn"]
)

if "dr_cr_flag" in silver_transactions_stage.columns:
    silver_transactions_stage = silver_transactions_stage.withColumn(
        "dr_cr_flag",
        F.when(F.col("dr_cr_flag").isin("D", "DR", "DEBIT"), "DEBIT")
         .when(F.col("dr_cr_flag").isin("C", "CR", "CREDIT"), "CREDIT")
         .otherwise(F.col("dr_cr_flag"))
    )

if "transaction_date" in silver_transactions_stage.columns:
    silver_transactions_stage = (
        silver_transactions_stage
        .withColumn("transaction_year", F.year(F.col("transaction_date")))
        .withColumn("transaction_month", F.month(F.col("transaction_date")))
        .withColumn("transaction_day", F.dayofmonth(F.col("transaction_date")))
    )

if "amount" in silver_transactions_stage.columns:
    silver_transactions_stage = silver_transactions_stage.withColumn(
        "txn_amount_bucket",
        F.when(F.col("amount") < 1000, "LOW")
         .when(F.col("amount") < 10000, "MEDIUM")
         .otherwise("HIGH")
    )

silver_transactions_stage = silver_transactions_stage.withColumn("silver_load_ts", F.current_timestamp())

display(silver_transactions_stage)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reject Transactions**

# CELL ********************

reject_transactions = silver_transactions_stage.withColumn(
    "reject_reason",
    F.when(F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == ""), "MISSING_TRANSACTION_ID")
     .when(F.col("account_id").isNull() | (F.trim(F.col("account_id")) == ""), "MISSING_ACCOUNT_ID")
     .when("amount" in silver_transactions_stage.columns and F.col("amount").isNull(), "INVALID_AMOUNT")
     .otherwise(None)
)

reject_transactions = reject_transactions.filter(F.col("reject_reason").isNotNull())

display(reject_transactions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Valid Transactions**

# CELL ********************

valid_transactions = silver_transactions_stage

if "transaction_id" in valid_transactions.columns:
    valid_transactions = valid_transactions.filter(F.col("transaction_id").isNotNull() & (F.trim(F.col("transaction_id")) != ""))

if "account_id" in valid_transactions.columns:
    valid_transactions = valid_transactions.filter(F.col("account_id").isNotNull() & (F.trim(F.col("account_id")) != ""))

display(valid_transactions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Deduplicate Transactions**

# CELL ********************

if "transaction_id" in valid_transactions.columns:
    order_cols = []
    if "transaction_timestamp" in valid_transactions.columns:
        order_cols.append(F.col("transaction_timestamp").desc_nulls_last())
    if "updated_at" in valid_transactions.columns:
        order_cols.append(F.col("updated_at").desc_nulls_last())
    if "created_at" in valid_transactions.columns:
        order_cols.append(F.col("created_at").desc_nulls_last())
    order_cols.append(F.col("silver_load_ts").desc_nulls_last())

    w = Window.partitionBy("transaction_id").orderBy(*order_cols)

    silver_transactions = (
        valid_transactions
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
else:
    silver_transactions = valid_transactions

display(silver_transactions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reference Validation**

# CELL ********************

silver_transactions_validated = silver_transactions

if "account_id" in silver_transactions.columns and table_exists("silver_accounts"):
    silver_transactions_validated = silver_transactions_validated.join(
        spark.table("silver_accounts").select("account_id").distinct(),
        on="account_id",
        how="inner"
    )

if "customer_id" in silver_transactions_validated.columns and table_exists("silver_customers"):
    silver_transactions_validated = silver_transactions_validated.join(
        spark.table("silver_customers").select("customer_id").distinct(),
        on="customer_id",
        how="left"
    )

if "branch_id" in silver_transactions_validated.columns and table_exists("silver_branches"):
    silver_transactions_validated = silver_transactions_validated.join(
        spark.table("silver_branches").select("branch_id").distinct(),
        on="branch_id",
        how="left"
    )

display(silver_transactions_validated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Transactions**

# CELL ********************

spark.sql("DROP TABLE IF EXISTS silver_transactions")
spark.sql("DROP TABLE IF EXISTS reject_transactions")

silver_transactions_validated.write.mode("overwrite").format("delta").saveAsTable("silver_transactions")
reject_transactions.write.mode("overwrite").format("delta").saveAsTable("reject_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Transactions**

# CELL ********************

spark.sql("SELECT COUNT(*) AS cnt FROM silver_transactions").show()
spark.sql("SELECT * FROM silver_transactions LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Loans Table from Bronze**

# CELL ********************

df_loans = spark.sql("""
SELECT *
FROM Bank_Bronze_Lakehouse.`dbo.loans`
""")

print(df_loans.columns)
display(df_loans)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **ALternate Names**

# CELL ********************

df_loans = rename_if_exists(df_loans, "loan_amt", "loan_amount")
df_loans = rename_if_exists(df_loans, "principal_amt", "principal_amount")
df_loans = rename_if_exists(df_loans, "disbursal_date", "disbursement_date")
df_loans = rename_if_exists(df_loans, "loan_status", "status")
df_loans = rename_if_exists(df_loans, "loan_tenure_months", "tenure_months")

print(df_loans.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transform Loans**

# CELL ********************

silver_loans_stage = df_loans

silver_loans_stage = trim_if_exists(
    silver_loans_stage,
    [
        "loan_id", "customer_id", "account_id", "branch_id",
        "loan_type", "status", "currency", "record_source"
    ]
)

silver_loans_stage = upper_if_exists(
    silver_loans_stage,
    ["loan_type", "status", "currency"]
)

silver_loans_stage = date_if_exists(
    silver_loans_stage,
    ["application_date", "approval_date", "disbursement_date", "start_date", "close_date", "maturity_date"]
)

silver_loans_stage = ts_if_exists(
    silver_loans_stage,
    ["created_at", "updated_at"]
)

silver_loans_stage = decimal_if_exists(
    silver_loans_stage,
    ["loan_amount", "principal_amount", "interest_rate", "emi_amount", "outstanding_amount", "overdue_amount"]
)

silver_loans_stage = int_if_exists(
    silver_loans_stage,
    ["tenure_months"]
)

if "tenure_months" in silver_loans_stage.columns:
    silver_loans_stage = silver_loans_stage.withColumn(
        "tenure_years",
        F.round(F.col("tenure_months") / 12, 2)
    )

if "disbursement_date" in silver_loans_stage.columns:
    silver_loans_stage = silver_loans_stage.withColumn(
        "loan_age_days",
        F.datediff(F.current_date(), F.col("disbursement_date"))
    )

if "status" in silver_loans_stage.columns:
    silver_loans_stage = silver_loans_stage.withColumn(
        "is_active_loan",
        F.when(F.col("status").isin("ACTIVE", "OPEN", "CURRENT"), 1).otherwise(0)
    )

silver_loans_stage = silver_loans_stage.withColumn("silver_load_ts", F.current_timestamp())

display(silver_loans_stage)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reject Loans**

# CELL ********************

reject_loans = silver_loans_stage.withColumn(
    "reject_reason",
    F.when(F.col("loan_id").isNull() | (F.trim(F.col("loan_id")) == ""), "MISSING_LOAN_ID")
     .when(F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""), "MISSING_CUSTOMER_ID")
     .otherwise(None)
)

reject_loans = reject_loans.filter(F.col("reject_reason").isNotNull())

display(reject_loans)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Valid Loans**

# CELL ********************

valid_loans = silver_loans_stage

if "loan_id" in valid_loans.columns:
    valid_loans = valid_loans.filter(F.col("loan_id").isNotNull() & (F.trim(F.col("loan_id")) != ""))

if "customer_id" in valid_loans.columns:
    valid_loans = valid_loans.filter(F.col("customer_id").isNotNull() & (F.trim(F.col("customer_id")) != ""))

display(valid_loans)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Deduplicate Loans**

# CELL ********************

if "loan_id" in valid_loans.columns:
    order_cols = []
    if "updated_at" in valid_loans.columns:
        order_cols.append(F.col("updated_at").desc_nulls_last())
    if "created_at" in valid_loans.columns:
        order_cols.append(F.col("created_at").desc_nulls_last())
    order_cols.append(F.col("silver_load_ts").desc_nulls_last())

    w = Window.partitionBy("loan_id").orderBy(*order_cols)

    silver_loans = (
        valid_loans
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
else:
    silver_loans = valid_loans

display(silver_loans)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reference Validation**

# CELL ********************

silver_loans_validated = silver_loans

if "customer_id" in silver_loans_validated.columns and table_exists("silver_customers"):
    silver_loans_validated = silver_loans_validated.join(
        spark.table("silver_customers").select("customer_id").distinct(),
        on="customer_id",
        how="inner"
    )

if "account_id" in silver_loans_validated.columns and table_exists("silver_accounts"):
    silver_loans_validated = silver_loans_validated.join(
        spark.table("silver_accounts").select("account_id").distinct(),
        on="account_id",
        how="left"
    )

if "branch_id" in silver_loans_validated.columns and table_exists("silver_branches"):
    silver_loans_validated = silver_loans_validated.join(
        spark.table("silver_branches").select("branch_id").distinct(),
        on="branch_id",
        how="left"
    )

display(silver_loans_validated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Loans**

# CELL ********************

spark.sql("DROP TABLE IF EXISTS silver_loans")
spark.sql("DROP TABLE IF EXISTS reject_loans")

silver_loans_validated.write.mode("overwrite").format("delta").saveAsTable("silver_loans")
reject_loans.write.mode("overwrite").format("delta").saveAsTable("reject_loans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate Loans**

# CELL ********************

spark.sql("SELECT COUNT(*) AS cnt FROM silver_loans").show()
spark.sql("SELECT * FROM silver_loans LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Code**

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

# MARKDOWN ********************

# **Parameters**

# CELL ********************

batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
source_system = "SQL_Server_Banking"
current_ts = F.current_timestamp()

print("Batch ID:", batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Source and Reference tables**

# CELL ********************

# Bronze source tables

df_balances_raw = spark.read.table("Bank_Bronze_Lakehouse.`dbo.balances_daily`")

df_loan_payments_raw = spark.read.table("Bank_Bronze_Lakehouse.`dbo.loan_payments`") 


# Existing silver reference/master tables

df_silver_accounts = spark.table("silver_accounts")

df_silver_customers = spark.table("silver_customers")

df_silver_branches = spark.table("silver_branches")

df_silver_loans = spark.table("silver_loans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Balances_Daily**

# CELL ********************

df_balances_std = (
    df_balances_raw
    .select(
        F.trim(F.col("account_id")).alias("account_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("branch_id")).alias("branch_id"),
        F.to_date("snapshot_date").alias("snapshot_date"),
        F.col("end_of_day_balance").cast(T.DecimalType(18, 2)).alias("end_of_day_balance"),
        F.trim(F.col("currency")).alias("currency"),
        F.trim(F.col("record_source")).alias("record_source")
    )
    .withColumn("load_ts", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("source_system", F.lit(source_system))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Check Duplicates in Balances**

# CELL ********************

dup_bal_window = Window.partitionBy("account_id", "snapshot_date").orderBy(F.col("load_ts").desc())

df_balances_dedup = (
    df_balances_std
    .withColumn("rn", F.row_number().over(dup_bal_window))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Join to reference tables for Validation**

# CELL ********************

df_balances_joined = (
    df_balances_dedup.alias("b")
    .join(df_silver_accounts.select("account_id").distinct().alias("a"), on="account_id", how="left")
    .join(df_silver_customers.select("customer_id").distinct().alias("c"), on="customer_id", how="left")
    .join(df_silver_branches.select("branch_id").distinct().alias("br"), on="branch_id", how="left")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create valid and reject balances rows**

# CELL ********************

today_date = datetime.today().date()

df_balances_valid = (
    df_balances_joined
    .filter(
        (F.col("rn") == 1) &
        (F.col("account_id").isNotNull()) &
        (F.col("snapshot_date").isNotNull()) &
        (F.col("end_of_day_balance").isNotNull()) &
        (F.col("customer_id").isNotNull()) &
        (F.col("branch_id").isNotNull())
    )
)

df_balances_reject = (
    df_balances_joined
    .withColumn(
        "reject_reason",
        F.when(F.col("rn") > 1, F.lit("DUPLICATE_ACCOUNT_SNAPSHOT"))
         .when(F.col("account_id").isNull(), F.lit("NULL_ACCOUNT_ID"))
         .when(F.col("snapshot_date").isNull(), F.lit("NULL_SNAPSHOT_DATE"))
         .when(F.col("end_of_day_balance").isNull(), F.lit("NULL_BALANCE"))
         .when(F.col("customer_id").isNull(), F.lit("INVALID_CUSTOMER_ID"))
         .when(F.col("branch_id").isNull(), F.lit("INVALID_BRANCH_ID"))
         .otherwise(F.lit("UNKNOWN_REJECT"))
    )
    .filter(F.col("reject_reason").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Silver_balances_daily**

# CELL ********************

(
    df_balances_valid
    .drop("rn")
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver_balances_daily")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Reject_balances_daily**

# CELL ********************

(
    df_balances_reject
    .drop("rn")
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("reject_balances_daily")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Silver_Loan_Payments**

# CELL ********************

(
    df_balances_reject
    .drop("rn")
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("reject_balances_daily")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_loan_payments_std = (
    df_loan_payments_raw
    .select(
        F.trim(F.col("payment_id")).alias("payment_id"),
        F.trim(F.col("loan_id")).alias("loan_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("branch_id")).alias("branch_id"),
        F.to_date("payment_date").alias("payment_date"),
        F.col("payment_amount").cast(T.DecimalType(18, 2)).alias("payment_amount"),
        F.upper(F.trim(F.col("status"))).alias("status"),
        F.trim(F.col("record_source")).alias("record_source")
    )
    .withColumn("load_ts", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("source_system", F.lit(source_system))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Deduplicate Loan_Payments**

# CELL ********************

dup_lp_window = Window.partitionBy("payment_id").orderBy(F.col("load_ts").desc())

df_loan_payments_dedup = (
    df_loan_payments_std
    .withColumn("rn", F.row_number().over(dup_lp_window))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validate against Reference table**

# CELL ********************

df_loan_payments_joined = (
    df_loan_payments_dedup.alias("lp")
    .join(df_silver_loans.select("loan_id").distinct().alias("l"), on="loan_id", how="left")
    .join(df_silver_customers.select("customer_id").distinct().alias("c"), on="customer_id", how="left")
    .join(df_silver_branches.select("branch_id").distinct().alias("br"), on="branch_id", how="left")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Split Valid/Reject Loans Payments**

# CELL ********************

valid_payment_statuses = ["SUCCESS", "FAILED", "PENDING"]

df_loan_payments_valid = (
    df_loan_payments_joined
    .filter(
        (F.col("rn") == 1) &
        (F.col("payment_id").isNotNull()) &
        (F.col("loan_id").isNotNull()) &
        (F.col("customer_id").isNotNull()) &
        (F.col("branch_id").isNotNull()) &
        (F.col("payment_date").isNotNull()) &
        (F.col("payment_amount").isNotNull()) &
        (F.col("payment_amount") > 0) &
        (F.col("status").isin(valid_payment_statuses))
    )
)

df_loan_payments_reject = (
    df_loan_payments_joined
    .withColumn(
        "reject_reason",
        F.when(F.col("rn") > 1, F.lit("DUPLICATE_PAYMENT_ID"))
         .when(F.col("payment_id").isNull(), F.lit("NULL_PAYMENT_ID"))
         .when(F.col("loan_id").isNull(), F.lit("INVALID_LOAN_ID"))
         .when(F.col("customer_id").isNull(), F.lit("INVALID_CUSTOMER_ID"))
         .when(F.col("branch_id").isNull(), F.lit("INVALID_BRANCH_ID"))
         .when(F.col("payment_date").isNull(), F.lit("NULL_PAYMENT_DATE"))
         .when(F.col("payment_amount").isNull(), F.lit("NULL_PAYMENT_AMOUNT"))
         .when(F.col("payment_amount") <= 0, F.lit("INVALID_PAYMENT_AMOUNT"))
         .when(~F.col("status").isin(valid_payment_statuses), F.lit("INVALID_PAYMENT_STATUS"))
         .otherwise(F.lit("UNKNOWN_REJECT"))
    )
    .filter(F.col("reject_reason").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Silver_Loan_Payments**

# CELL ********************

(
    df_loan_payments_valid
    .drop("rn")
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver_loan_payments")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Write Reject_Loan_Payments**

# CELL ********************

(
    df_loan_payments_reject
    .drop("rn")
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("reject_loan_payments")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Customer_Current**

# CELL ********************

(
    df_silver_customers
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver_customer_current")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create Account_Current**

# CELL ********************

(
    df_silver_accounts
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver_account_current")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Data Quality Metrics**

# CELL ********************

dq_rows = [
    ("silver_balances_daily", "VALID_ROW_COUNT", df_balances_valid.count(), batch_id),
    ("reject_balances_daily", "REJECT_ROW_COUNT", df_balances_reject.count(), batch_id),
    ("silver_loan_payments", "VALID_ROW_COUNT", df_loan_payments_valid.count(), batch_id),
    ("reject_loan_payments", "REJECT_ROW_COUNT", df_loan_payments_reject.count(), batch_id),
]

dq_schema = T.StructType([
    T.StructField("table_name", T.StringType(), True),
    T.StructField("rule_name", T.StringType(), True),
    T.StructField("row_count", T.LongType(), True),
    T.StructField("batch_id", T.StringType(), True),
])

df_dq_results = spark.createDataFrame(dq_rows, schema=dq_schema) \
    .withColumn("execution_ts", F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Data Quality results**

# CELL ********************

(
    df_dq_results
    .write
    .mode("append")
    .format("delta")
    .saveAsTable("dq_results")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for tbl in [
    "silver_balances_daily",
    "reject_balances_daily",
    "silver_loan_payments",
    "reject_loan_payments",
    "silver_customer_current",
    "silver_account_current",
    "dq_results"
]:
    print(f"\nTable: {tbl}")
    spark.sql(f"SELECT COUNT(*) AS cnt FROM {tbl}").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM silver_balances_daily LIMIT 10").show(truncate=False)
spark.sql("SELECT * FROM silver_loan_payments LIMIT 10").show(truncate=False)
spark.sql("SELECT * FROM dq_results ORDER BY execution_ts DESC LIMIT 20").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **SCD2 - CUSTOMERS**

# CELL ********************

from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from datetime import datetime
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Batch Metadata**

# CELL ********************

batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
source_system = "SQL_Server_Banking"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Read Customers Table Bronze**

# CELL ********************

df_customers_base = spark.read.table("Bank_Bronze_Lakehouse.`dbo.customers`") 
df_customers_delta = spark.read.table("Bank_Bronze_Lakehouse.`dbo.customers_delta`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Standardize Customer & Base Data**

# CELL ********************

def standardize_customers(df):
    return (
        df.select(
            F.trim("customer_id").alias("customer_id"),
            F.trim("full_name").alias("full_name"),
            F.to_date("dob").alias("dob"),
            F.upper(F.trim("gender")).alias("gender"),
            F.trim("pan").alias("pan"),
            F.trim("phone").alias("phone"),
            F.trim("email").alias("email"),
            F.trim("address").alias("address"),
            F.trim("city").alias("city"),
            F.trim("state").alias("state"),
            F.upper(F.trim("segment")).alias("segment"),
            F.upper(F.trim("risk_rating")).alias("risk_rating"),
            F.trim("home_branch_id").alias("home_branch_id"),
            F.upper(F.trim("kyc_status")).alias("kyc_status"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            F.trim("record_source").alias("record_source")
        )
    )

df_customers_base_std = standardize_customers(df_customers_base).withColumn("src_priority", F.lit(1))
df_customers_delta_std = standardize_customers(df_customers_delta).withColumn("src_priority", F.lit(2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Latest source row per Customer ID**

# CELL ********************

df_customers_all = df_customers_base_std.unionByName(df_customers_delta_std)

cust_window = Window.partitionBy("customer_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("src_priority").desc()
)

df_customer_latest = (
    df_customers_all
    .withColumn("rn", F.row_number().over(cust_window))
    .filter(F.col("rn") == 1)
    .drop("rn", "src_priority")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add HASH & SCD2 Fields**

# CELL ********************

df_customer_latest = (
    df_customer_latest
    .withColumn(
        "hash_value",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("full_name"), F.lit("")),
                F.coalesce(F.col("dob").cast("string"), F.lit("")),
                F.coalesce(F.col("gender"), F.lit("")),
                F.coalesce(F.col("pan"), F.lit("")),
                F.coalesce(F.col("phone"), F.lit("")),
                F.coalesce(F.col("email"), F.lit("")),
                F.coalesce(F.col("address"), F.lit("")),
                F.coalesce(F.col("city"), F.lit("")),
                F.coalesce(F.col("state"), F.lit("")),
                F.coalesce(F.col("segment"), F.lit("")),
                F.coalesce(F.col("risk_rating"), F.lit("")),
                F.coalesce(F.col("home_branch_id"), F.lit("")),
                F.coalesce(F.col("kyc_status"), F.lit(""))
            ),
            256
        )
    )
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to", F.lit("9999-12-31").cast("date"))
    .withColumn("is_current", F.lit(1))
    .withColumn("load_ts", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("source_system", F.lit(source_system))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create Table**

# CELL ********************

table_name = "silver_customer_scd2"

if not spark.catalog.tableExists(table_name):
    (
        df_customer_latest
        .withColumn("customer_sk", F.monotonically_increasing_id())
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(table_name)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **SCD Merge Logic**

# CELL ********************

if spark.catalog.tableExists(table_name):
    target = DeltaTable.forName(spark, table_name)

    df_current = spark.table(table_name).filter("is_current = 1")

    df_changes = (
        df_customer_latest.alias("s")
        .join(
            df_current.select("customer_id", "hash_value", "customer_sk").alias("t"),
            on="customer_id",
            how="left"
        )
        .withColumn(
            "change_type",
            F.when(F.col("t.customer_id").isNull(), F.lit("NEW"))
             .when(F.col("s.hash_value") != F.col("t.hash_value"), F.lit("CHANGED"))
             .otherwise(F.lit("NOCHANGE"))
        )
    )

    # Expire old rows where changed
    df_changed_keys = df_changes.filter("change_type = 'CHANGED'").select("customer_id").distinct()

    if df_changed_keys.count() > 0:
        (
            target.alias("t")
            .merge(
                df_changed_keys.alias("s"),
                "t.customer_id = s.customer_id AND t.is_current = 1"
            )
            .whenMatchedUpdate(set={
                "effective_to": "current_date() - INTERVAL 1 DAY",
                "is_current": "0"
            })
            .execute()
        )

    # Insert NEW + CHANGED rows
    df_inserts = (
        df_changes
        .filter("change_type IN ('NEW','CHANGED')")
        .select("s.*")
        .withColumn("customer_sk", F.monotonically_increasing_id())
    )

    if df_inserts.count() > 0:
        (
            df_inserts
            .write
            .mode("append")
            .format("delta")
            .saveAsTable(table_name)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create CUrrent Snapshot Date**

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE silver_customer_current AS
SELECT *
FROM silver_customer_scd2
WHERE is_current = 1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Notebook code — Accounts SCD2**

# CELL ********************

df_accounts_base = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.accounts`") 

df_accounts_delta = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.accounts_delta`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Standardize Accounts**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T

def standardize_accounts(df):
    return (
        df.select(
            F.trim("account_id").alias("account_id"),
            F.trim("customer_id").alias("customer_id"),
            F.trim("branch_id").alias("branch_id"),
            F.upper(F.trim("account_type")).alias("account_type"),
            F.upper(F.trim("currency")).alias("currency"),
            F.to_date("open_date").alias("open_date"),
            F.upper(F.trim("status")).alias("status"),
            F.col("overdraft_limit").cast(T.DecimalType(18, 2)).alias("overdraft_limit"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            F.trim("record_source").alias("record_source")
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Keep latest source row per account_id **

# CELL ********************

df_accounts_base_std = standardize_accounts(df_accounts_base).withColumn("src_priority", F.lit(1))

df_accounts_delta_std = standardize_accounts(df_accounts_delta).withColumn("src_priority", F.lit(2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_accounts_all = df_accounts_base_std.unionByName(df_accounts_delta_std)

acct_window = Window.partitionBy("account_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("src_priority").desc()
)

df_account_latest = (
    df_accounts_all
    .withColumn("rn", F.row_number().over(acct_window))
    .filter(F.col("rn") == 1)
    .drop("rn", "src_priority")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add hash and SCD fields**

# CELL ********************

df_account_latest = (
    df_account_latest
    .withColumn(
        "hash_value",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("customer_id"), F.lit("")),
                F.coalesce(F.col("branch_id"), F.lit("")),
                F.coalesce(F.col("account_type"), F.lit("")),
                F.coalesce(F.col("currency"), F.lit("")),
                F.coalesce(F.col("open_date").cast("string"), F.lit("")),
                F.coalesce(F.col("status"), F.lit("")),
                F.coalesce(F.col("overdraft_limit").cast("string"), F.lit(""))
            ),
            256
        )
    )
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to", F.lit("9999-12-31").cast("date"))
    .withColumn("is_current", F.lit(1))
    .withColumn("load_ts", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("source_system", F.lit(source_system))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Initial Load**

# CELL ********************

acct_table_name = "silver_account_scd2"

if not spark.catalog.tableExists(acct_table_name):
    (
        df_account_latest
        .withColumn("account_sk", F.monotonically_increasing_id())
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(acct_table_name)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Merge logic**

# CELL ********************

if spark.catalog.tableExists(acct_table_name):
    target = DeltaTable.forName(spark, acct_table_name)

    df_current = spark.table(acct_table_name).filter("is_current = 1")

    df_changes = (
        df_account_latest.alias("s")
        .join(
            df_current.select("account_id", "hash_value", "account_sk").alias("t"),
            on="account_id",
            how="left"
        )
        .withColumn(
            "change_type",
            F.when(F.col("t.account_id").isNull(), F.lit("NEW"))
             .when(F.col("s.hash_value") != F.col("t.hash_value"), F.lit("CHANGED"))
             .otherwise(F.lit("NOCHANGE"))
        )
    )

    df_changed_keys = df_changes.filter("change_type = 'CHANGED'").select("account_id").distinct()

    if df_changed_keys.count() > 0:
        (
            target.alias("t")
            .merge(
                df_changed_keys.alias("s"),
                "t.account_id = s.account_id AND t.is_current = 1"
            )
            .whenMatchedUpdate(set={
                "effective_to": "current_date() - INTERVAL 1 DAY",
                "is_current": "0"
            })
            .execute()
        )

    df_inserts = (
        df_changes
        .filter("change_type IN ('NEW','CHANGED')")
        .select("s.*")
        .withColumn("account_sk", F.monotonically_increasing_id())
    )

    if df_inserts.count() > 0:
        (
            df_inserts
            .write
            .mode("append")
            .format("delta")
            .saveAsTable(acct_table_name)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Current Account Table**

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE silver_account_current AS
SELECT *
FROM silver_account_scd2
WHERE is_current = 1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

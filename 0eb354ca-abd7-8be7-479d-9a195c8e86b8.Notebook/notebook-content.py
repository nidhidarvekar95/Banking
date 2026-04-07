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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bank_Silver_Lakehouse.dim_date")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Setup**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def overwrite_delta_table(df, table_name):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    df.write.mode("overwrite").format("delta").saveAsTable(table_name)
    print(f"Loaded {table_name}")
    spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_name}").show()

def build_scd2(
    df,
    business_key,
    order_col,
    compare_cols,
    start_col_name="effective_start_date",
    end_col_name="effective_end_date",
    current_flag_name="is_current"
):
    """
    Build SCD2 history from a dataframe that can contain multiple versions per business key.
    Keeps a new version only when tracked attributes actually change.
    """

    # Create hash of tracked columns
    df_hashed = df.withColumn(
        "_scd_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in compare_cols]
            ),
            256
        )
    )

    w = Window.partitionBy(business_key).orderBy(F.col(order_col).asc_nulls_last())

    df_versioned = (
        df_hashed
        .withColumn("_prev_hash", F.lag("_scd_hash").over(w))
        .withColumn(
            "_is_changed",
            F.when(F.col("_prev_hash").isNull(), 1)
             .when(F.col("_prev_hash") != F.col("_scd_hash"), 1)
             .otherwise(0)
        )
        .filter(F.col("_is_changed") == 1)
        .drop("_prev_hash", "_is_changed")
    )

    w2 = Window.partitionBy(business_key).orderBy(F.col(order_col).asc_nulls_last())

    df_scd2 = (
        df_versioned
        .withColumn(start_col_name, F.col(order_col).cast("timestamp"))
        .withColumn("_next_start", F.lead(F.col(order_col)).over(w2))
        .withColumn(
            end_col_name,
            F.when(
                F.col("_next_start").isNull(),
                F.lit(None).cast("timestamp")
            ).otherwise(F.expr("_next_start - INTERVAL 1 SECONDS"))
        )
        .withColumn(
            current_flag_name,
            F.when(F.col("_next_start").isNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .drop("_next_start", "_scd_hash")
    )

    return df_scd2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Customers: merge base + delta, clean, deduplicate**

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

# CELL ********************

from pyspark.sql import functions as F

df_customers = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.customers`")
df_customers_delta = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.customers_delta`")

customers_all = df_customers.unionByName(df_customers_delta)

silver_customers_raw = (
    customers_all
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("full_name", F.trim(F.col("full_name")))
    .withColumn("dob", F.to_date(F.col("dob")))
    .withColumn("gender", F.upper(F.trim(F.col("gender"))))
    .withColumn("pan", F.upper(F.trim(F.col("pan"))))
    .withColumn("phone", F.trim(F.col("phone")))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("address", F.trim(F.col("address")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("segment", F.upper(F.trim(F.col("segment"))))
    .withColumn("risk_rating", F.upper(F.trim(F.col("risk_rating"))))
    .withColumn("home_branch_id", F.trim(F.col("home_branch_id")))
    .withColumn("kyc_status", F.upper(F.trim(F.col("kyc_status"))))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
    .withColumn("record_source", F.trim(F.col("record_source")))
    .withColumn("silver_load_ts", F.current_timestamp())
    .filter(F.col("customer_id").isNotNull())
)

# Choose event time for SCD ordering
silver_customers_raw = silver_customers_raw.withColumn(
    "scd_event_ts",
    F.coalesce(F.col("updated_at"), F.col("created_at"), F.current_timestamp())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_compare_cols = [
    "full_name", "dob", "gender", "pan", "phone", "email", "address",
    "city", "state", "segment", "risk_rating", "home_branch_id", "kyc_status"
]

silver_customer_scd2 = build_scd2(
    df=silver_customers_raw,
    business_key="customer_id",
    order_col="scd_event_ts",
    compare_cols=customer_compare_cols
)

display(silver_customer_scd2)
overwrite_delta_table(silver_customer_scd2, "silver_customer_scd2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Branches**

# CELL ********************

from pyspark.sql import functions as F

df_branches = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.branches`")

silver_branches = (
    df_branches
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("branch_name", F.trim(F.col("branch_name")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("region", F.upper(F.trim(F.col("region"))))
    .withColumn("opened_date", F.to_date(F.col("opened_date")))
    .withColumn("is_active", F.col("is_active").cast("int"))
    .withColumn("silver_load_ts", F.current_timestamp())
)

reject_branches = silver_branches.filter(F.col("branch_id").isNull())
silver_branches_valid = silver_branches.filter(F.col("branch_id").isNotNull())

display(silver_branches_valid)

silver_branches_valid.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_silver_lakehouse.silver_branches")

reject_branches.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_silver_lakehouse.reject_branches")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Accounts: merge base + delta, validate against customers and branches**

# CELL ********************

df_accounts = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.accounts`")
df_accounts_delta = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.accounts_delta`")

accounts_all = df_accounts.unionByName(df_accounts_delta)

silver_accounts_raw = (
    accounts_all
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
    .filter(F.col("account_id").isNotNull())
)

# Validate 
silver_accounts_raw = (
    silver_accounts_raw
    .join(spark.table("silver_customer_scd2").select("customer_id").distinct(), on="customer_id", how="inner")
    .join(spark.table("silver_branches").select("branch_id").distinct(), on="branch_id", how="inner")
)

silver_accounts_raw = silver_accounts_raw.withColumn(
    "scd_event_ts",
    F.coalesce(F.col("updated_at"), F.col("created_at"), F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

account_compare_cols = [
    "customer_id", "branch_id", "account_type", "currency",
    "status", "open_date", "overdraft_limit"
]

silver_account_scd2 = build_scd2(
    df=silver_accounts_raw,
    business_key="account_id",
    order_col="scd_event_ts",
    compare_cols=account_compare_cols
)

display(silver_account_scd2)
overwrite_delta_table(silver_account_scd2, "silver_account_scd2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cards**

# CELL ********************

from pyspark.sql import functions as F

df_cards = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.cards`")

silver_cards_raw = (
    df_cards
    .withColumn("card_id", F.trim(F.col("card_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("linked_account_id", F.trim(F.col("linked_account_id")))
    .withColumn("card_type", F.upper(F.trim(F.col("card_type"))))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("issued_date", F.to_date(F.col("issued_date")))
    .withColumn("credit_limit", F.col("credit_limit").cast("decimal(18,2)"))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
    .withColumn("record_source", F.trim(F.col("record_source")))
    .withColumn("silver_load_ts", F.current_timestamp())
)

valid_customers = (
    spark.table("silver_customers")
    .select(F.col("customer_id").alias("lk_customer_id"))
    .distinct()
)

valid_accounts = (
    spark.table("silver_accounts")
    .select(F.col("account_id").alias("lk_account_id"))
    .distinct()
)

silver_cards_check = (
    silver_cards_raw
    .join(
        valid_customers,
        silver_cards_raw["customer_id"] == valid_customers["lk_customer_id"],
        how="left"
    )
    .join(
        valid_accounts,
        silver_cards_raw["linked_account_id"] == valid_accounts["lk_account_id"],
        how="left"
    )
)

silver_cards_valid = (
    silver_cards_check
    .filter(
        F.col("lk_customer_id").isNotNull() &
        F.col("lk_account_id").isNotNull()
    )
    .drop("lk_customer_id", "lk_account_id")
)

reject_cards = (
    silver_cards_check
    .filter(
        F.col("lk_customer_id").isNull() |
        F.col("lk_account_id").isNull()
    )
    .drop("lk_customer_id", "lk_account_id")
)

display(silver_cards_valid)

silver_cards_valid.write.mode("overwrite").format("delta").saveAsTable("silver_cards")
reject_cards.write.mode("overwrite").format("delta").saveAsTable("reject_cards")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Channels**

# CELL ********************

df_transactions = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.transactions`")

silver_channels = (
    df_transactions
    .select(F.upper(F.trim(F.col("channel"))).alias("channel_name"))
    .filter(F.col("channel_name").isNotNull())
    .distinct()
    .withColumn("silver_load_ts", F.current_timestamp())
)

silver_channels.write.mode("overwrite").format("delta").saveAsTable("silver_channels")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transaction Types**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_transactions = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.transactions`")

silver_transactions_raw = (
    df_transactions
    .withColumn("transaction_id", F.trim(F.col("transaction_id")))
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("txn_type", F.upper(F.trim(F.col("txn_type"))))
    .withColumn("channel", F.upper(F.trim(F.col("channel"))))
    .withColumn("direction", F.upper(F.trim(F.col("direction"))))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("merchant_category", F.upper(F.trim(F.col("merchant_category"))))
    .withColumn("counterparty", F.trim(F.col("counterparty")))
    .withColumn("is_fraud_label", F.col("is_fraud_label").cast("int"))
    .withColumn("transaction_ts", F.to_timestamp(F.col("transaction_ts")))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("record_source", F.trim(F.col("record_source")))
    .withColumn("silver_load_ts", F.current_timestamp())
)

w_txn = Window.partitionBy("transaction_id").orderBy(F.col("transaction_ts").desc_nulls_last())

silver_transactions_valid = (
    silver_transactions_raw
    .withColumn("rn", F.row_number().over(w_txn))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .join(spark.table("silver_accounts").select("account_id").distinct(), on="account_id", how="inner")
    .join(spark.table("silver_customers").select("customer_id").distinct(), on="customer_id", how="inner")
    .join(spark.table("silver_branches").select("branch_id").distinct(), on="branch_id", how="inner")
)

display(silver_transactions_valid)

spark.sql("DROP TABLE IF EXISTS silver_transactions")

silver_transactions_valid.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_transactions = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.transactions`")

silver_txn_types = (
    df_transactions
    .select(F.upper(F.trim(F.col("txn_type"))).alias("txn_type_name"))
    .filter(F.col("txn_type_name").isNotNull())
    .distinct()
    .withColumn("silver_load_ts", F.current_timestamp())
)

display(silver_txn_types)

spark.sql("DROP TABLE IF EXISTS silver_txn_types")

silver_txn_types.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_txn_types")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transactions: clean and validate**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_transactions = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.transactions`")

silver_transactions_raw = (
    df_transactions
    .withColumn("transaction_id", F.trim(F.col("transaction_id")))
    .withColumn("transaction_ts", F.to_timestamp(F.col("transaction_ts")))
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("txn_type", F.upper(F.trim(F.col("txn_type"))))
    .withColumn("channel", F.upper(F.trim(F.col("channel"))))
    .withColumn("direction", F.upper(F.trim(F.col("direction"))))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("merchant_category", F.upper(F.trim(F.col("merchant_category"))))
    .withColumn("counterparty", F.trim(F.col("counterparty")))
    .withColumn("is_fraud_label", F.col("is_fraud_label").cast("int"))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("record_source", F.trim(F.col("record_source")))
    .withColumn("silver_load_ts", F.current_timestamp())
)

w_txn = Window.partitionBy("transaction_id").orderBy(F.col("transaction_ts").desc_nulls_last())

silver_transactions_dedup = (
    silver_transactions_raw
    .withColumn("rn", F.row_number().over(w_txn))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .alias("t")
)

valid_accounts = (
    spark.table("silver_accounts")
    .select("account_id")
    .distinct()
    .alias("a")
)

valid_customers = (
    spark.table("silver_customers")
    .select("customer_id")
    .distinct()
    .alias("c")
)

valid_branches = (
    spark.table("silver_branches")
    .select("branch_id")
    .distinct()
    .alias("b")
)

valid_channels = (
    spark.table("silver_channels")
    .select(F.col("channel_name").alias("lk_channel"))
    .distinct()
    .alias("ch")
)

valid_txn_types = (
    spark.table("silver_txn_types")
    .select(F.col("txn_type_name").alias("lk_txn_type"))
    .distinct()
    .alias("tt")
)

silver_transactions_valid = (
    silver_transactions_dedup
    .join(valid_accounts, F.col("t.account_id") == F.col("a.account_id"), "inner")
    .join(valid_customers, F.col("t.customer_id") == F.col("c.customer_id"), "inner")
    .join(valid_branches, F.col("t.branch_id") == F.col("b.branch_id"), "inner")
    .join(valid_channels, F.col("t.channel") == F.col("ch.lk_channel"), "inner")
    .join(valid_txn_types, F.col("t.txn_type") == F.col("tt.lk_txn_type"), "inner")
    .select("t.*")
)

display(silver_transactions_valid)

spark.sql("DROP TABLE IF EXISTS silver_transactions")

silver_transactions_valid.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Daily Balances**

# CELL ********************

df_balances = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.balances_daily`")

silver_balances_daily_raw = (
    df_balances
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("snapshot_date", F.to_date(F.col("snapshot_date")))
    .withColumn("end_of_day_balance", F.col("end_of_day_balance").cast("decimal(18,2)"))
    .withColumn("silver_load_ts", F.current_timestamp())
)

silver_balances_daily_valid = (
    silver_balances_daily_raw
    .join(spark.table("silver_accounts").select("account_id").distinct(), on="account_id", how="inner")
    .join(spark.table("silver_customers").select("customer_id").distinct(), on="customer_id", how="inner")
    .join(spark.table("silver_branches").select("branch_id").distinct(), on="branch_id", how="inner")
)
silver_balances_daily_valid.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver_balances_daily")
# silver_balances_daily_valid.write.mode("overwrite").format("delta").saveAsTable("silver_balances_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Loans**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def overwrite_delta_table(df, table_name):
    (
        df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

def build_scd2(df, business_key, order_col, compare_cols):
    df1 = df.withColumn(
        "attr_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in compare_cols]
            ),
            256
        )
    )

    w = Window.partitionBy(business_key).orderBy(order_col)

    df1 = df1.withColumn("prev_hash", F.lag("attr_hash").over(w))

    df_changes = df1.filter(
        F.col("prev_hash").isNull() | (F.col("attr_hash") != F.col("prev_hash"))
    )

    w2 = Window.partitionBy(business_key).orderBy(order_col)

    df_scd2 = (
        df_changes
        .withColumn("effective_from", F.col(order_col))
        .withColumn("next_effective_from", F.lead(order_col).over(w2))
        .withColumn(
            "effective_to",
            F.expr("next_effective_from - INTERVAL 1 MICROSECOND")
        )
        .withColumn(
            "is_current",
            F.when(F.col("next_effective_from").isNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .drop("prev_hash", "next_effective_from")
    )

    return df_scd2


df_loans = spark.sql("SELECT * FROM Bank_Bronze_Lakehouse.`dbo.loans`")

silver_loans_raw = (
    df_loans
    .withColumn("loan_id", F.trim(F.col("loan_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("branch_id", F.trim(F.col("branch_id")))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("start_date", F.to_date(F.col("start_date")))
    .withColumn("interest_rate_apr", F.col("interest_rate_apr").cast("decimal(10,4)"))
    .withColumn("principal_amount", F.col("principal_amount").cast("decimal(18,2)"))
    .withColumn("tenure_months", F.col("tenure_months").cast("int"))
    .withColumn("silver_load_ts", F.current_timestamp())
    .filter(F.col("loan_id").isNotNull())
)

silver_loans_raw = (
    silver_loans_raw
    .join(
        spark.table("silver_customer_scd2").select("customer_id").distinct(),
        on="customer_id",
        how="inner"
    )
    .join(
        spark.table("silver_branches").select("branch_id").distinct(),
        on="branch_id",
        how="inner"
    )
)

silver_loans_raw = silver_loans_raw.withColumn("scd_event_ts", F.current_timestamp())

loan_compare_cols = [
    "customer_id",
    "branch_id",
    "status",
    "start_date",
    "interest_rate_apr",
    "principal_amount",
    "tenure_months"
]

silver_loan_scd2 = build_scd2(
    df=silver_loans_raw,
    business_key="loan_id",
    order_col="scd_event_ts",
    compare_cols=loan_compare_cols
)

display(silver_loan_scd2)
overwrite_delta_table(silver_loan_scd2, "silver_loan_scd2")

df_active_loans = (
    silver_loan_scd2
    .filter(F.col("is_current") == 1)
    .select(
        "loan_id",
        "customer_id",
        "branch_id",
        "start_date",
        "principal_amount",
        "tenure_months",
        "interest_rate_apr",
        "status"
    )
)

silver_loan_payments = (
    df_active_loans
    .filter(F.col("start_date").isNotNull())
    .filter(F.col("tenure_months").isNotNull())
    .filter(F.col("tenure_months") > 0)
    .filter(F.col("principal_amount").isNotNull())
    .withColumn("installment_no", F.explode(F.sequence(F.lit(1), F.col("tenure_months"))))
    .withColumn("payment_date", F.add_months(F.col("start_date"), F.col("installment_no") - 1))
    .withColumn("payment_amount", F.round(F.col("principal_amount") / F.col("tenure_months"), 2))
    .withColumn("payment_id", F.concat(F.col("loan_id"), F.lit("_"), F.col("installment_no")))
    .withColumn(
        "status",
        F.when(F.col("payment_date") <= F.current_date(), F.lit("PAID"))
         .otherwise(F.lit("PENDING"))
    )
    .withColumn("silver_load_ts", F.current_timestamp())
    .select(
        "payment_id",
        "loan_id",
        "customer_id",
        "branch_id",
        "installment_no",
        "payment_date",
        "payment_amount",
        "status",
        "silver_load_ts"
    )
)

display(silver_loan_payments)
overwrite_delta_table(silver_loan_payments, "silver_loan_payments")

spark.table("silver_loan_payments").select(
    F.min(F.to_date("payment_date")).alias("min_payment_date"),
    F.max(F.to_date("payment_date")).alias("max_payment_date"),
    F.count("*").alias("row_cnt"),
    F.countDistinct(F.to_date("payment_date")).alias("distinct_payment_dates")
).show()

spark.table("silver_loan_payments") \
    .groupBy(F.to_date("payment_date").alias("payment_dt")) \
    .agg(
        F.count("*").alias("rows"),
        F.sum("payment_amount").alias("total_payment")
    ) \
    .orderBy("payment_dt") \
    .show(100, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

loan_compare_cols = [
    "customer_id", "branch_id", "status", "start_date",
    "interest_rate_apr", "principal_amount", "tenure_months"
]

silver_loan_scd2 = build_scd2(
    df=silver_loans_raw,
    business_key="loan_id",
    order_col="scd_event_ts",
    compare_cols=loan_compare_cols
)

display(silver_loan_scd2)
overwrite_delta_table(silver_loan_scd2, "silver_loan_scd2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

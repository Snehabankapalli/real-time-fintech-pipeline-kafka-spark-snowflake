from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

transaction_schema = StructType([
    StructField("transaction_id",    StringType(), nullable=False),
    StructField("account_id",        StringType(), nullable=False),
    StructField("event_type",        StringType(), nullable=False),
    StructField("amount",            DoubleType(), nullable=False),
    StructField("currency",          StringType(), nullable=False),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("event_timestamp",   StringType(), nullable=False),
    StructField("failure_type",      StringType(), nullable=True),
])

VALID_EVENT_TYPES = {"authorization", "settlement", "refund", "chargeback"}

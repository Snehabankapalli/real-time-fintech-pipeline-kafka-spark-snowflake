from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    """
    Deduplicate by transaction_id, keeping the most recently processed record.
    Uses a window function so the most recent processed_at wins on ties.
    """
    window = Window.partitionBy("transaction_id").orderBy(col("processed_at").desc())
    return (
        df.withColumn("_rank", row_number().over(window))
          .filter(col("_rank") == 1)
          .drop("_rank")
    )


def deduplicate_stream(df: DataFrame) -> DataFrame:
    """
    In-stream deduplication using Spark Structured Streaming dropDuplicates.
    Operates within a watermark window — does not catch cross-batch duplicates.
    Pair with downstream Snowflake MERGE for full exactly-once guarantee.
    """
    return df.dropDuplicates(["transaction_id"])

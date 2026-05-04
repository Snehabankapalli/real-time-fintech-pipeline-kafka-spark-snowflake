import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_json, struct


VALID_EVENT_TYPES = {"authorization", "settlement", "refund", "chargeback"}


def classify_failure(row: dict) -> str:
    """Return a human-readable failure reason for a bad record."""
    if not row.get("transaction_id"):
        return "missing_transaction_id"
    if not row.get("account_id"):
        return "missing_account_id"
    if row.get("amount") is None or not isinstance(row.get("amount"), (int, float)):
        return "invalid_amount"
    if row.get("amount", 0) < 0:
        return "negative_amount"
    if not row.get("event_timestamp"):
        return "missing_timestamp"
    if row.get("event_type") not in VALID_EVENT_TYPES:
        return f"invalid_event_type:{row.get('event_type')}"
    return "unknown"


def route_to_dlq(invalid_df: DataFrame, kafka_bootstrap: str, dlq_topic: str) -> None:
    """Write invalid records to the DLQ Kafka topic with failure_reason attached."""
    dlq_df = (
        invalid_df
        .withColumn("failure_reason", lit("schema_validation_failed"))
        .withColumn("dlq_timestamp", current_timestamp())
        .select(to_json(struct("*")).alias("value"))
    )

    (
        dlq_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", dlq_topic)
        .option("checkpointLocation", "/tmp/dlq-checkpoint")
        .outputMode("append")
        .start()
    )

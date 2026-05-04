import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

DLQ_TOPIC   = os.getenv("KAFKA_DLQ_TOPIC", "card-transactions-dlq")
MAIN_TOPIC  = os.getenv("KAFKA_TOPIC", "card-transactions")
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    DLQ_TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="dlq-replay-group",
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

VALID_EVENT_TYPES = {"authorization", "settlement", "refund", "chargeback"}


def repair_event(event: dict) -> dict | None:
    """
    Attempt to repair a DLQ event. Returns None if the event is not repairable.
    """
    event = {k: v for k, v in event.items() if k not in ("failure_type", "failure_reason", "dlq_timestamp")}

    if not event.get("transaction_id"):
        return None  # Cannot repair — no stable identity

    if event.get("event_type") not in VALID_EVENT_TYPES:
        return None  # Unknown event type — cannot safely default

    if not isinstance(event.get("amount"), (int, float)) or event.get("amount", 0) < 0:
        return None  # Cannot infer correct amount

    if not event.get("event_timestamp"):
        event["event_timestamp"] = datetime.now(timezone.utc).isoformat()

    event["replayed_at"] = datetime.now(timezone.utc).isoformat()
    return event


if __name__ == "__main__":
    print(f"Reading from DLQ: {DLQ_TOPIC}")
    replayed = 0
    skipped = 0

    for msg in consumer:
        original = msg.value
        repaired = repair_event(original)

        if repaired:
            producer.send(MAIN_TOPIC, repaired)
            replayed += 1
            print(f"REPLAYED [{replayed}]: {repaired['transaction_id']}")
        else:
            skipped += 1
            print(f"SKIPPED  [{skipped}]: not repairable — {original.get('failure_type', 'unknown')}")

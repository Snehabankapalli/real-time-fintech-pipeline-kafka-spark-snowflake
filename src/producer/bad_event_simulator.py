import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "card-transactions"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

FAILURE_TYPES = [
    "missing_transaction_id",
    "bad_amount",
    "missing_timestamp",
    "unknown_event_type",
    "negative_amount",
]


def corrupt_event(event: dict) -> dict:
    failure_type = random.choice(FAILURE_TYPES)

    if failure_type == "missing_transaction_id":
        event.pop("transaction_id", None)

    elif failure_type == "bad_amount":
        event["amount"] = "not_a_number"

    elif failure_type == "missing_timestamp":
        event.pop("event_timestamp", None)

    elif failure_type == "unknown_event_type":
        event["event_type"] = "mystery_event"

    elif failure_type == "negative_amount":
        event["amount"] = -abs(event["amount"])

    event["failure_type"] = failure_type
    return event


def generate_base_event() -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"acct_{random.randint(10000, 99999)}",
        "event_type": random.choice(["authorization", "settlement", "refund", "chargeback"]),
        "amount": round(random.uniform(1, 500), 2),
        "currency": "USD",
        "merchant_category": random.choice(["grocery", "travel", "fuel", "restaurant"]),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    print("Sending bad events to card-transactions topic (10% corrupt rate)...")
    while True:
        event = generate_base_event()
        if random.random() < 0.10:
            event = corrupt_event(event)
            print(f"BAD:  {event}")
        else:
            print(f"GOOD: {event['transaction_id']}")
        producer.send(TOPIC, event)
        time.sleep(0.1)

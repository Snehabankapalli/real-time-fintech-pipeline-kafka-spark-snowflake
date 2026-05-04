import pytest
from src.spark.dlq_handler import classify_failure


def test_missing_transaction_id():
    assert classify_failure({"account_id": "acct_1", "amount": 10.0, "event_type": "authorization", "event_timestamp": "2026-05-04T00:00:00Z"}) == "missing_transaction_id"


def test_missing_account_id():
    assert classify_failure({"transaction_id": "txn_1", "amount": 10.0, "event_type": "authorization", "event_timestamp": "2026-05-04T00:00:00Z"}) == "missing_account_id"


def test_string_amount():
    assert classify_failure({"transaction_id": "txn_1", "account_id": "acct_1", "amount": "bad", "event_type": "authorization", "event_timestamp": "2026-05-04T00:00:00Z"}) == "invalid_amount"


def test_negative_amount():
    assert classify_failure({"transaction_id": "txn_1", "account_id": "acct_1", "amount": -5.0, "event_type": "authorization", "event_timestamp": "2026-05-04T00:00:00Z"}) == "negative_amount"


def test_invalid_event_type():
    result = classify_failure({"transaction_id": "txn_1", "account_id": "acct_1", "amount": 10.0, "event_type": "mystery", "event_timestamp": "2026-05-04T00:00:00Z"})
    assert result.startswith("invalid_event_type")


def test_missing_timestamp():
    assert classify_failure({"transaction_id": "txn_1", "account_id": "acct_1", "amount": 10.0, "event_type": "authorization"}) == "missing_timestamp"


def test_valid_event_returns_unknown():
    """A fully valid event should not be classified as a failure."""
    result = classify_failure({
        "transaction_id": "txn_1",
        "account_id": "acct_1",
        "amount": 10.0,
        "event_type": "authorization",
        "event_timestamp": "2026-05-04T00:00:00Z",
    })
    assert result == "unknown"

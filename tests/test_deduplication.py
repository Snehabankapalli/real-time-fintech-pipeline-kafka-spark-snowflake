import pytest
from datetime import datetime, timezone


def deduplicate(events: list[dict], key: str = "transaction_id") -> list[dict]:
    """Simple in-memory deduplication — mirrors the Spark logic."""
    seen = {}
    for event in events:
        k = event.get(key)
        if k and k not in seen:
            seen[k] = event
    return list(seen.values())


def test_no_duplicates_unchanged():
    events = [
        {"transaction_id": "txn_1", "amount": 10.0},
        {"transaction_id": "txn_2", "amount": 20.0},
    ]
    result = deduplicate(events)
    assert len(result) == 2


def test_exact_duplicate_removed():
    events = [
        {"transaction_id": "txn_1", "amount": 10.0},
        {"transaction_id": "txn_1", "amount": 10.0},
    ]
    result = deduplicate(events)
    assert len(result) == 1
    assert result[0]["transaction_id"] == "txn_1"


def test_first_occurrence_wins():
    events = [
        {"transaction_id": "txn_1", "amount": 10.0},
        {"transaction_id": "txn_1", "amount": 99.0},  # duplicate with different amount
    ]
    result = deduplicate(events)
    assert len(result) == 1
    assert result[0]["amount"] == 10.0


def test_missing_key_excluded():
    events = [
        {"transaction_id": "txn_1", "amount": 10.0},
        {"amount": 20.0},  # no transaction_id
    ]
    result = deduplicate(events)
    assert len(result) == 1


def test_empty_input():
    assert deduplicate([]) == []


def test_single_event():
    events = [{"transaction_id": "txn_1", "amount": 5.0}]
    assert len(deduplicate(events)) == 1

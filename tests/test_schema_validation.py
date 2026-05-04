import pytest


VALID_EVENT_TYPES = {"authorization", "settlement", "refund", "chargeback"}
REQUIRED_FIELDS   = ["transaction_id", "account_id", "event_type", "amount", "event_timestamp"]


def make_event(**overrides):
    base = {
        "transaction_id": "txn_abc123",
        "account_id": "acct_99999",
        "event_type": "authorization",
        "amount": 42.50,
        "currency": "USD",
        "event_timestamp": "2026-05-04T12:00:00Z",
    }
    base.update(overrides)
    return base


def is_valid(event: dict) -> bool:
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            return False
    if event.get("event_type") not in VALID_EVENT_TYPES:
        return False
    if not isinstance(event.get("amount"), (int, float)) or event["amount"] <= 0:
        return False
    return True


def test_valid_event_passes():
    assert is_valid(make_event()) is True


def test_missing_transaction_id_fails():
    assert is_valid(make_event(transaction_id=None)) is False


def test_invalid_event_type_fails():
    assert is_valid(make_event(event_type="mystery_event")) is False


def test_string_amount_fails():
    assert is_valid(make_event(amount="not_a_number")) is False


def test_negative_amount_fails():
    assert is_valid(make_event(amount=-10.00)) is False


def test_missing_timestamp_fails():
    assert is_valid(make_event(event_timestamp=None)) is False


@pytest.mark.parametrize("event_type", ["authorization", "settlement", "refund", "chargeback"])
def test_all_valid_event_types_pass(event_type):
    assert is_valid(make_event(event_type=event_type)) is True

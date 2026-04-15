"""Tests for TransactionEvent model and TransactionProducer."""

import pytest
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.producer.transaction_producer import TransactionEvent, TRANSACTION_SCHEMA


class TestTransactionEventCreate:
    def test_creates_with_required_fields(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=5000,
            merchant_id="MERCH-456",
        )
        assert event.account_id == "ACC-123"
        assert event.amount_cents == 5000
        assert event.merchant_id == "MERCH-456"

    def test_transaction_id_is_valid_uuid(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=1000,
            merchant_id="MERCH-001",
        )
        # Should not raise
        parsed = uuid.UUID(event.transaction_id)
        assert str(parsed) == event.transaction_id

    def test_default_currency_is_usd(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=1000,
            merchant_id="MERCH-001",
        )
        assert event.currency == "USD"

    def test_default_event_type_is_authorization(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=1000,
            merchant_id="MERCH-001",
        )
        assert event.event_type == "AUTHORIZATION"

    def test_default_is_international_false(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=1000,
            merchant_id="MERCH-001",
        )
        assert event.is_international is False

    def test_default_metadata_is_empty_dict(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=1000,
            merchant_id="MERCH-001",
        )
        assert event.metadata == {}

    def test_event_timestamp_is_iso_format(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=1000,
            merchant_id="MERCH-001",
        )
        # Should parse without error
        parsed = datetime.fromisoformat(event.event_timestamp)
        assert parsed is not None

    def test_custom_currency(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=10000,
            merchant_id="MERCH-999",
            currency="EUR",
        )
        assert event.currency == "EUR"

    def test_custom_metadata(self):
        meta = {"pos_terminal": "T-001", "region": "west"}
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=100,
            merchant_id="MERCH-001",
            metadata=meta,
        )
        assert event.metadata == meta

    def test_zero_amount_allowed(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=0,
            merchant_id="MERCH-001",
        )
        assert event.amount_cents == 0

    def test_international_flag_set(self):
        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=50000,
            merchant_id="MERCH-INTL",
            is_international=True,
        )
        assert event.is_international is True

    def test_each_event_has_unique_transaction_id(self):
        events = [
            TransactionEvent.create("ACC-1", 100, "M-1")
            for _ in range(10)
        ]
        ids = {e.transaction_id for e in events}
        assert len(ids) == 10


class TestTransactionSchema:
    def test_schema_is_valid_json_string(self):
        import json
        schema = json.loads(TRANSACTION_SCHEMA)
        assert schema["type"] == "record"
        assert schema["name"] == "Transaction"

    def test_schema_has_required_fields(self):
        import json
        schema = json.loads(TRANSACTION_SCHEMA)
        field_names = {f["name"] for f in schema["fields"]}
        required = {
            "transaction_id", "account_id", "amount_cents",
            "currency", "merchant_id", "event_type", "event_timestamp",
        }
        assert required.issubset(field_names)

    def test_amount_cents_is_long_type(self):
        import json
        schema = json.loads(TRANSACTION_SCHEMA)
        amount_field = next(f for f in schema["fields"] if f["name"] == "amount_cents")
        assert amount_field["type"] == "long"

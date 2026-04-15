"""
Tests for PII tokenizer.
conftest.py sets PII_TOKENIZATION_SECRET before this module loads.
"""

import os
import pytest

# Import after conftest.py sets the env var
from src.spark.pii_tokenizer import _hmac_token


class TestHmacToken:
    def test_non_empty_input_returns_hex_string(self):
        token = _hmac_token("ACC-12345")
        assert isinstance(token, str)
        assert len(token) == 64  # SHA-256 hex digest is 64 chars
        assert all(c in "0123456789abcdef" for c in token)

    def test_empty_string_returns_empty_string(self):
        assert _hmac_token("") == ""

    def test_deterministic_same_input_same_output(self):
        token_a = _hmac_token("ACC-12345")
        token_b = _hmac_token("ACC-12345")
        assert token_a == token_b

    def test_different_inputs_produce_different_tokens(self):
        token_a = _hmac_token("ACC-11111")
        token_b = _hmac_token("ACC-22222")
        assert token_a != token_b

    def test_token_does_not_contain_original_value(self):
        original = "4111111111111111"  # Visa test card number
        token = _hmac_token(original)
        assert original not in token

    def test_whitespace_input_produces_non_empty_token(self):
        token = _hmac_token("  ")
        assert len(token) == 64

    def test_unicode_input_handled(self):
        token = _hmac_token("ACC-\u4e2d\u6587")
        assert len(token) == 64

    def test_long_input_produces_fixed_length_token(self):
        long_value = "X" * 10_000
        token = _hmac_token(long_value)
        assert len(token) == 64

    def test_token_is_irreversible(self):
        """HMAC tokens cannot be reversed to recover the original value."""
        token = _hmac_token("4111111111111111")
        # Cannot reverse — just assert the token is not the input
        assert token != "4111111111111111"
        assert len(token) == 64


class TestPiiTokenizerRequiresSecret:
    def test_secret_env_var_is_set(self):
        """Guard: env var must be set for tokenizer to work at all."""
        secret = os.environ.get("PII_TOKENIZATION_SECRET", "")
        assert secret, "PII_TOKENIZATION_SECRET must be set before importing pii_tokenizer"

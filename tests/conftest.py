"""
Conftest for fintech pipeline tests.
PII_TOKENIZATION_SECRET must be set before pii_tokenizer is imported
because it reads the env var at module load time.
"""

import os

# Set before any test module imports pii_tokenizer
os.environ.setdefault("PII_TOKENIZATION_SECRET", "test-secret-for-pytest-only")

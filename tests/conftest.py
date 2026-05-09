"""
Conftest for fintech pipeline tests.

- Sets PII_TOKENIZATION_SECRET before pii_tokenizer loads (it reads env at import time).
- Injects lightweight sys.modules mocks for pyspark and confluent_kafka when those
  packages are not installed (e.g., CI without a JVM / Kafka broker). The unit tests
  only exercise pure-Python logic; they never execute real Spark or Kafka code paths.
"""

import os
import sys
from unittest.mock import MagicMock

# Must be set before pii_tokenizer is imported (reads env var at module load time)
os.environ.setdefault("PII_TOKENIZATION_SECRET", "test-secret-for-pytest-only")


def _mock_pyspark() -> None:
    """Register a minimal pyspark module tree in sys.modules."""
    sql_functions = MagicMock(name="pyspark.sql.functions")
    for fn in ("col", "current_timestamp", "lit", "to_json", "struct", "udf",
               "when", "coalesce", "length", "regexp_replace", "trim"):
        setattr(sql_functions, fn, MagicMock())

    sql_types = MagicMock(name="pyspark.sql.types")
    for t in ("StringType", "BooleanType", "IntegerType", "LongType",
              "DoubleType", "TimestampType", "StructType", "StructField"):
        setattr(sql_types, t, MagicMock())

    sql = MagicMock(name="pyspark.sql")
    sql.functions = sql_functions
    sql.types = sql_types
    sql.DataFrame = MagicMock()

    pyspark = MagicMock(name="pyspark")
    pyspark.sql = sql

    sys.modules.setdefault("pyspark", pyspark)
    sys.modules.setdefault("pyspark.sql", sql)
    sys.modules.setdefault("pyspark.sql.functions", sql_functions)
    sys.modules.setdefault("pyspark.sql.types", sql_types)


def _mock_confluent_kafka() -> None:
    """Register a minimal confluent_kafka module tree in sys.modules."""
    serialization = MagicMock(name="confluent_kafka.serialization")
    serialization.SerializationContext = MagicMock()
    serialization.MessageField = MagicMock()

    avro = MagicMock(name="confluent_kafka.schema_registry.avro")
    avro.AvroSerializer = MagicMock()

    schema_registry = MagicMock(name="confluent_kafka.schema_registry")
    schema_registry.SchemaRegistryClient = MagicMock()
    schema_registry.avro = avro

    ck = MagicMock(name="confluent_kafka")
    ck.Producer = MagicMock()
    ck.schema_registry = schema_registry
    ck.serialization = serialization

    sys.modules.setdefault("confluent_kafka", ck)
    sys.modules.setdefault("confluent_kafka.schema_registry", schema_registry)
    sys.modules.setdefault("confluent_kafka.schema_registry.avro", avro)
    sys.modules.setdefault("confluent_kafka.serialization", serialization)


try:
    import pyspark  # noqa: F401
except ImportError:
    _mock_pyspark()

try:
    import confluent_kafka  # noqa: F401
except ImportError:
    _mock_confluent_kafka()

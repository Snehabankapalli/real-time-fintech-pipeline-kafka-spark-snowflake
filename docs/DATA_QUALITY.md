# Data Quality

## Layer 1: Kafka Schema Registry

Avro schema enforced at the Kafka producer.
Any event that does not match the schema is rejected before it enters the topic.

```json
{
  "type": "record",
  "name": "CardTransaction",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "account_id",     "type": "string"},
    {"name": "event_type",     "type": {"type": "enum", "symbols": ["authorization","settlement","refund","chargeback"]}},
    {"name": "amount",         "type": "double"},
    {"name": "currency",       "type": "string"},
    {"name": "event_timestamp","type": "string"}
  ]
}
```

## Layer 2: Spark Validation

In-stream checks before any record reaches Snowflake:

```python
valid_df = (
    parsed_df
    .filter(col("transaction_id").isNotNull())
    .filter(col("amount") > 0)
    .filter(col("event_type").isin("authorization", "settlement", "refund", "chargeback"))
    .filter(col("event_timestamp").isNotNull())
    .dropDuplicates(["transaction_id"])
)
```

Failed records → DLQ with failure reason attached.

## Layer 3: dbt Tests

Run after every Snowflake load. Tests block downstream marts if they fail.

```yaml
models:
  - name: fct_transactions
    columns:
      - name: transaction_id
        tests: [not_null, unique]
      - name: account_token
        tests: [not_null]
      - name: event_type
        tests:
          - accepted_values:
              values: [authorization, settlement, refund, chargeback]
      - name: amount
        tests: [not_null]
    tests:
      - dbt_utils.recency:
          datepart: minute
          field: processed_at
          interval: 30
```

Custom test — no negative settled amounts:
```sql
select transaction_id
from {{ ref('fct_transactions') }}
where event_type = 'settlement' and amount < 0
```

## Layer 4: Freshness SLA

Snowflake table freshness monitored every 15 minutes via dbt source freshness:

```yaml
sources:
  - name: raw
    tables:
      - name: card_transactions
        freshness:
          warn_after: {count: 15, period: minute}
          error_after: {count: 60, period: minute}
```

Freshness breach triggers Slack alert.

## Metrics Tracked

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| DLQ rate | < 0.01% | > 0.1% |
| Null rate (transaction_id) | 0% | > 0% |
| Duplicate rate | 0% | > 0% |
| Table freshness | < 15 min | > 60 min |
| dbt test pass rate | 100% | < 100% |

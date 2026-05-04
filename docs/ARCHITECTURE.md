# Architecture

## System Overview

Real-time credit card transaction pipeline processing 1M+ events/day with sub-second latency.

```
Card Events (authorizations, settlements, refunds, chargebacks)
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  Apache Kafka (AWS MSK)                               │
│  Topics: card-transactions / card-transactions-dlq    │
│  Schema Registry: Avro enforcement                    │
│  Partitions: 12 (by account_id hash)                  │
└────────────────────┬──────────────────────────────────┘
                     │
                     ▼
┌───────────────────────────────────────────────────────┐
│  PySpark Structured Streaming (AWS EMR Serverless)    │
│                                                       │
│  1. Parse JSON → schema enforcement                   │
│  2. Validate required fields → DLQ on failure         │
│  3. Tokenize account_id (HMAC-SHA256)                 │
│  4. Deduplicate by transaction_id                     │
│  5. Enrich: merchant category, flags                  │
│  6. Write to Snowflake (micro-batch, 10s)             │
└────────────────────┬──────────────────────────────────┘
                     │
                     ▼
┌───────────────────────────────────────────────────────┐
│  Snowflake                                            │
│  RAW          → append-only, 90-day retention        │
│  STAGING      → stg_card_transactions (dbt)          │
│  INTERMEDIATE → int_transaction_enriched (dbt)       │
│  MARTS        → fct_transactions                     │
│                 rpt_cfpb_monthly                     │
│                 rpt_executive_kpis                   │
└────────────────────┬──────────────────────────────────┘
                     │
                     ▼
┌───────────────────────────────────────────────────────┐
│  Consumers                                            │
│  BI Dashboards · Fraud Detection · CFPB Filing        │
└───────────────────────────────────────────────────────┘

Observability
  Prometheus (consumer lag, DLQ rate, latency)
  Slack alerts (DLQ spikes, SLA breaches, job failures)
```

## Key Design Decisions

### Why Kafka over SQS or Kinesis?
- Replay capability: consumers can rewind and reprocess events from any offset
- Multiple independent consumers on the same stream (Spark + fraud detection + audit)
- Exactly-once delivery with idempotent producers
- Kafka UI for real-time visibility into consumer lag

### Why Spark Structured Streaming over Flink?
- Native Python SDK (PySpark) vs Java-first Flink
- Strong Snowflake connector ecosystem
- EMR Serverless makes Spark ops-free at scale
- Team already owns PySpark for batch — one skill set

### Why 10-second micro-batches?
- Sub-second latency is not required for this use case (fraud decisions happen in auth layer)
- 10s batches reduce Snowflake write amplification by 10x vs per-event writes
- Still provides near-real-time for BI dashboards and regulatory reporting

### Why EMR Serverless?
- No cluster management — scales to zero between jobs
- Cost: pay per vCPU-second, not per idle hour
- 83% batch runtime reduction vs legacy fixed cluster

## Partitioning Strategy

Kafka topics partitioned by `account_id` hash (12 partitions).
This ensures all events for a given account land in the same partition — enabling
in-order processing and efficient deduplication without cross-partition shuffles.

Snowflake tables clustered by `event_timestamp::date`.
Queries filtered by date (most reporting queries) skip unrelated micro-partitions automatically.

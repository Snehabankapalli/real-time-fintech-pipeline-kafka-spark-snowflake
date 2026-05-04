# Real-Time Fintech Data Platform

> Kafka → Spark Structured Streaming → Snowflake → dbt → Observability

Processes real-time credit card events for analytics, fraud detection, and CFPB-style regulatory reporting. Built for 1M+ events/day, sub-second processing latency, and 99.9% uptime.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![AWS EMR](https://img.shields.io/badge/AWS_EMR-FF9900?style=flat&logo=amazonaws&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
[![CI](https://github.com/Snehabankapalli/real-time-fintech-pipeline-kafka-spark-snowflake/actions/workflows/ci.yml/badge.svg)](https://github.com/Snehabankapalli/real-time-fintech-pipeline-kafka-spark-snowflake/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## What This System Does

Ingests real-time financial transaction events from a credit card platform, processes them through a streaming pipeline, and delivers clean, tokenized, deduplicated data to Snowflake for analytics, CFPB regulatory reporting, and fraud detection.

- **High-throughput ingestion** — 1M+ card events/day (authorizations, settlements, refunds, chargebacks) via Kafka MSK
- **Sub-second processing** — PySpark Structured Streaming on AWS EMR Serverless with 10-second micro-batches
- **Production reliability** — Dead-letter queues, schema registry, exactly-once semantics, automated DLQ alerting
- **Compliance-ready** — PII tokenized at ingestion (HMAC-SHA256), CFPB monthly reports auto-generated from dbt marts
- **Cost-optimized** — EMR Serverless reduces pipeline cost by 83% vs fixed cluster; auto-suspend Snowflake cuts warehouse spend 10x

---

## Architecture

```
Card Events (authorizations, settlements, refunds, chargebacks)
        │
        ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  Apache Kafka (AWS MSK)                                                   │
│  Topics: card-transactions (12 partitions) / card-transactions-dlq        │
│  Schema Registry: Avro enforcement, rejects malformed events at boundary  │
└────────────────────┬──────────────────────────────────────────────────────┘
                     │
                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  PySpark Structured Streaming (AWS EMR Serverless)                        │
│                                                                           │
│  1. Parse JSON → schema enforcement (src/spark/schema.py)                 │
│  2. Validate required fields → DLQ on failure (src/spark/dlq_handler.py)  │
│  3. Tokenize account_id HMAC-SHA256 (src/spark/pii_tokenizer.py)          │
│  4. Deduplicate by transaction_id (src/spark/deduplication.py)            │
│  5. Enrich: merchant flags, latency (src/spark/streaming_job.py)          │
│  6. Write to Snowflake RAW schema (10-second micro-batch)                 │
└────────────────────┬──────────────────────────────────────────────────────┘
                     │
                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  Snowflake                                                                │
│  RAW          → append-only, 90-day S3 retention                         │
│  STAGING      → stg_card_transactions (type cast, dedupe, rename)        │
│  INTERMEDIATE → int_transaction_enriched (flags, tiers, dates)           │
│  MARTS        → fct_transactions (incremental, clustered by event_date)  │
│                 rpt_cfpb_monthly (regulatory report)                     │
│                 rpt_executive_kpis (daily dashboard)                     │
└────────────────────┬──────────────────────────────────────────────────────┘
                     │
                     ▼
            BI Dashboards · Fraud Detection · CFPB Filing

Observability: Prometheus (consumer lag, DLQ rate, p99 latency) + Slack alerts
```

Full architecture detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## Scale Assumptions

| Metric | Target |
|--------|--------|
| Daily events | 1M+ |
| p99 end-to-end latency | < 2 seconds |
| Pipeline uptime | 99.9% SLA |
| DLQ rate | < 0.01% of events |
| Snowflake freshness | < 15 minutes |
| Batch time (vs legacy) | 83% reduction |

---

## Failure Scenarios Handled

| Scenario | Handling |
|----------|----------|
| Schema mismatch | Routed to DLQ with failure reason. Replay via `src/replay/replay_dlq.py`. |
| Duplicate transactions | `dropDuplicates` in Spark + MERGE on Snowflake — exactly-once guaranteed. |
| Snowflake write failure | Spark checkpoint preserves Kafka offset. Auto-retry with exponential backoff. |
| Kafka consumer lag spike | Prometheus alert at lag > 1000. Scale EMR Serverless max workers. |
| Late-arriving data | 2-hour Spark watermark + 3-day dbt incremental lookback window. |
| PII tokenization failure | Job fails fast — no degraded mode where raw PII could reach Snowflake. |

Full failure runbook: [docs/FAILURE_SCENARIOS.md](docs/FAILURE_SCENARIOS.md)

---

## Data Quality

**Layer 1 — Kafka Schema Registry:** Avro schema enforced at producer. Malformed events rejected before entering topic.

**Layer 2 — Spark validation:** Required fields checked in-stream. Invalid records routed to DLQ, never reach Snowflake.

**Layer 3 — dbt tests:** Run after every load. Block downstream marts on failure.

```yaml
fct_transactions:
  - transaction_id: [not_null, unique]
  - account_token:  [not_null]
  - event_type:     [accepted_values: authorization/settlement/refund/chargeback]
  - amount_usd:     [not_null]
  - freshness:      warn > 15 min, error > 60 min
```

**Layer 4 — Custom test:** `dbt/tests/assert_no_negative_settled_amounts.sql`

Full data quality spec: [docs/DATA_QUALITY.md](docs/DATA_QUALITY.md)

---

## Observability

Prometheus metrics exposed at `:9000/metrics`:

| Metric | Alert Threshold |
|--------|----------------|
| `kafka_consumer_lag_total` | > 1000 for 5 min |
| `dlq_events_per_minute` | > 10 for 2 min |
| `pipeline_latency_p99_seconds` | > 5s for 5 min |
| `events_processed_per_second` | < 200 for 10 min |

Slack alerts fire to `#data-oncall` on any critical threshold breach.

---

## Cost Optimization

| Decision | Impact |
|----------|--------|
| EMR Serverless vs fixed cluster | ~$415/month savings per pipeline |
| Snowflake auto-suspend (60s) | 10x reduction in warehouse spend |
| 10-second micro-batch | 10x fewer Snowflake write transactions |
| dbt incremental (3-day lookback) | 83% reduction in compute vs full refresh |
| S3 Glacier for data > 90 days | $4/TB vs $40/TB in Snowflake |

Full cost breakdown: [docs/COST_OPTIMIZATION.md](docs/COST_OPTIMIZATION.md)

---

## How to Run

```bash
# 1. Start local Kafka stack
make up
# Kafka UI: http://localhost:8080 | Prometheus: http://localhost:9090

# 2. Install dependencies
pip install -r requirements.txt
cp .env.example .env  # fill in your values

# 3. Produce events (good + bad to test DLQ)
make producer       # clean events
make bad-producer   # 10% corrupt events

# 4. Run Spark consumer
make spark

# 5. Replay DLQ
make replay-dlq

# 6. Run dbt transformations
make dbt-run && make dbt-test

# 7. Run tests
make test
```

---

## Project Structure

```
.
├── src/
│   ├── producer/         # Kafka event generator + bad event simulator
│   ├── spark/            # Streaming job, schema, PII tokenizer, deduplication, DLQ handler
│   ├── monitoring/       # Prometheus metrics, Prometheus alert rules, Slack notifier
│   └── replay/           # DLQ inspector and replay script
├── dbt/
│   ├── models/
│   │   ├── staging/      # stg_card_transactions
│   │   ├── intermediate/ # int_transaction_enriched
│   │   └── marts/        # fct_transactions, rpt_cfpb_monthly, rpt_executive_kpis
│   └── tests/            # assert_no_negative_settled_amounts
├── tests/                # Unit tests: schema, PII, deduplication, DLQ handler
├── docs/
│   ├── ARCHITECTURE.md
│   ├── FAILURE_SCENARIOS.md
│   ├── COST_OPTIMIZATION.md
│   ├── DATA_QUALITY.md
│   └── INTERVIEW_TALKING_POINTS.md
├── docker-compose.yml    # Kafka + Zookeeper + Kafka UI + Prometheus
├── Makefile              # One-command dev workflow
└── .env.example
```

---

## Interview Talking Points

Five things a hiring manager should ask you about:

1. **Exactly-once semantics** — idempotent producer + Spark checkpoint + Snowflake MERGE on primary key
2. **DLQ design** — not a graveyard, a queue. Bad events are repaired and replayed with version-controlled logic
3. **PII strategy** — HMAC-SHA256 at the Spark layer, account_id dropped from schema before Snowflake write, deterministic tokenization enables cross-table joins on token
4. **Cost vs latency tradeoff** — 10-second micro-batch reduces Snowflake write transactions 10x with only 9 seconds of additional latency (acceptable for BI/reporting use case)
5. **Failure recovery** — Spark checkpoint preserves Kafka offsets, so any Snowflake failure replays from last commit automatically. No manual intervention needed.

Full talking points: [docs/INTERVIEW_TALKING_POINTS.md](docs/INTERVIEW_TALKING_POINTS.md)

---

## Related Portfolio Systems

- [Data Engineering Observability Platform](https://github.com/Snehabankapalli/data-engineering-observability-platform) — monitoring layer built for platforms like this
- [Modern Data Platform Migration](https://github.com/Snehabankapalli/modern-data-platform-migration) — batch migration patterns that complement this streaming system
- [HIPAA-Compliant Data Lake](https://github.com/Snehabankapalli/hipaa-data-lake-aws) — regulated healthcare variant of this architecture
- [GenAI Data Engineering Portfolio](https://github.com/Snehabankapalli/genai-de-portfolio) — AI tooling for intelligent pipeline management

---

## Contributing

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for setup, workflow, and code style.

---

## License

MIT License — see [LICENSE](LICENSE) for details.

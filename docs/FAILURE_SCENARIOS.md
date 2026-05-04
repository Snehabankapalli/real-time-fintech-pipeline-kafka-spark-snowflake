# Failure Scenarios

## 1. Malformed Event (Schema Mismatch)

**Trigger:** Producer sends event with missing required field or wrong type.

**Detection:** Spark schema enforcement — `from_json()` returns null for invalid records.

**Handling:**
- Invalid records routed to `card-transactions-dlq` topic
- DLQ message includes original payload + failure reason
- Prometheus counter incremented: `dlq_rate_per_minute`
- Slack alert fires if DLQ rate exceeds 10 events/min

**Recovery:** `src/replay/replay_dlq.py` — inspect, repair, replay to main topic.

---

## 2. Duplicate Transaction

**Trigger:** Producer retries on network timeout, sends same event twice.

**Detection:** `dropDuplicates(["transaction_id"])` in Spark streaming job.

**Handling:** Deduplication applied in-stream before write. Checkpoint location
preserves state across restarts so duplicates across micro-batches are caught.

**Impact:** Zero — duplicates never reach Snowflake.

---

## 3. Snowflake Write Failure

**Trigger:** Snowflake warehouse suspended, network timeout, or auth failure.

**Detection:** Spark write exception caught in `snowflake_writer.py`.

**Handling:**
- Spark checkpoint preserves Kafka offsets — no data loss
- Job retries write with exponential backoff (3x, 30s intervals)
- After 3 failures, Slack alert fires with full error context
- Airflow SLA miss alert triggers if Snowflake table freshness exceeds 60 min

**Recovery:** Fix Snowflake issue (resume warehouse, fix credentials) → Spark
replays from last committed Kafka offset automatically.

---

## 4. Kafka Consumer Lag Spike

**Trigger:** Spark job slow, EMR cluster undersized, or burst in event volume.

**Detection:** Prometheus `kafka_consumer_lag` metric.

**Alert:** Fires at lag > 1000 events for 5 minutes.

**Handling:**
- Short-term: increase Spark parallelism (`maxOffsetsPerTrigger`)
- Medium-term: scale EMR Serverless max workers
- Long-term: review micro-batch interval and partition count

---

## 5. Late-Arriving Data

**Trigger:** Mobile client buffers events offline, sends batch hours later.

**Detection:** `event_timestamp` is far behind `processed_at`.

**Handling:**
- Spark watermark set to 2 hours — events older than watermark are dropped
- dbt incremental models use a 3-day lookback window for marts
- CFPB monthly report uses `event_timestamp` not `processed_at` — late data
  lands in the correct reporting month

---

## 6. PII Tokenization Failure

**Trigger:** `PII_TOKENIZATION_SECRET` env var missing or corrupted.

**Detection:** `tokenize_account_id()` raises `ValueError` on startup.

**Handling:** Spark job fails fast on startup before processing any records.
No raw PII ever reaches Snowflake — the job does not degrade gracefully here by design.

**Recovery:** Fix env var → restart job.

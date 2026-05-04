# Interview Talking Points

## Why Kafka over SQS or Kinesis?

"Kafka gives replay. If a Spark job has a bug and writes bad data, you can fix the job
and replay from any Kafka offset — no data loss. SQS deletes messages on consumption.
Kinesis has a 7-day retention limit. For a financial pipeline where correctness is
critical, replay capability is non-negotiable.

We also run multiple independent consumers on the same topic — fraud detection, the
analytics Spark job, and an audit log consumer. Kafka fan-out handles this natively.
SQS requires separate queues per consumer."

## Why Spark Structured Streaming over Flink?

"Flink is technically superior for stateful stream processing at microsecond latency.
We did not need microsecond latency — 10-second micro-batches were acceptable.
Spark gave us: PySpark (team already knows it), a mature Snowflake connector,
EMR Serverless for ops-free scaling, and the ability to reuse the same codebase
for our batch jobs. One technology for streaming and batch reduces cognitive overhead."

## How do you achieve exactly-once semantics?

"Three things working together:
1. Idempotent Kafka producer — retries do not create duplicates in the topic.
2. Spark checkpointing — commits Kafka offsets only after a successful Snowflake write.
   If the write fails, Spark replays from the last committed offset.
3. Snowflake MERGE on transaction_id — even if Spark replays and sends the same record
   twice, Snowflake merges on the primary key and does not create duplicates."

## How does your DLQ design work?

"Bad records go to `card-transactions-dlq` with a `failure_reason` field attached.
The DLQ is not a graveyard — it is a queue for records that need human review or
automated repair.

`replay_dlq.py` reads from the DLQ, applies repair logic (filling in defaults,
correcting types), and sends the repaired event back to the main topic.
The repair logic is version-controlled and tested. Every replayed event gets a
`replayed_at` timestamp so we can track replay volume in the monitoring dashboard."

## How do you handle PII?

"HMAC-SHA256 tokenization at the Spark layer, before any record reaches Snowflake.
The tokenization secret is injected via environment variable — never hardcoded.
The original account_id is dropped from the schema after tokenization.

HMAC is deterministic — the same account_id always produces the same token.
This lets us join on account_token across tables without ever storing the raw ID.
Reversing the token requires the secret, which only the security team holds."

## What would you do differently at 10x scale?

"At 10x (10M+ events/day):
1. Increase Kafka partitions from 12 to 120 — more parallelism.
2. Move from micro-batch to continuous streaming if latency SLA tightens.
3. Separate Snowflake warehouses for ingestion vs BI — prevent resource contention.
4. Introduce Iceberg or Delta Lake for cheaper S3-based storage at the raw layer.
5. Consider Flink for stateful aggregations that need sub-second window results."

## How did you achieve 83% batch time reduction?

"The original pipeline was a fixed EMR cluster running a single-threaded Python job
that processed files sequentially. We replaced it with:
1. Parallel PySpark reads across all source files simultaneously.
2. EMR Serverless autoscaling — 20 executors during peak, scales down after.
3. Pushed the transformation layer from Python (pandas) into Spark — leveraged
   columnar execution and predicate pushdown.
4. Eliminated a redundant intermediate S3 write that was serializing the pipeline.

Result: 24-hour batch → 4-hour batch with no change to output correctness."

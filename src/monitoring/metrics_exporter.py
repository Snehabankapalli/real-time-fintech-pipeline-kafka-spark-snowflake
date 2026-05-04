import os
import time
import random
from prometheus_client import start_http_server, Gauge, Counter

PORT = int(os.getenv("PROMETHEUS_PORT", 9000))

consumer_lag      = Gauge("kafka_consumer_lag_total",     "Kafka consumer lag (events)")
dlq_rate          = Gauge("dlq_events_per_minute",        "DLQ events written per minute")
pipeline_latency  = Gauge("pipeline_latency_p99_seconds", "Pipeline end-to-end p99 latency (seconds)")
throughput        = Gauge("events_processed_per_second",  "Events processed per second")
snowflake_credits = Gauge("snowflake_credits_used_today", "Snowflake credits consumed today")

events_total      = Counter("events_processed_total",     "Total events processed")
dlq_total         = Counter("dlq_events_total",           "Total events routed to DLQ")


def collect_metrics():
    """
    In production: replace with real Kafka AdminClient and Snowflake queries.
    This simulates realistic metric values for demo purposes.
    """
    consumer_lag.set(random.randint(50, 800))
    dlq_rate.set(round(random.uniform(0.0, 3.0), 2))
    pipeline_latency.set(round(random.uniform(0.4, 1.8), 3))
    throughput.set(random.randint(800, 1200))
    snowflake_credits.set(round(random.uniform(1.2, 8.5), 2))
    events_total.inc(random.randint(100, 200))


if __name__ == "__main__":
    start_http_server(PORT)
    print(f"Prometheus metrics server running on :{PORT}/metrics")
    while True:
        collect_metrics()
        time.sleep(10)

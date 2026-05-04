# Cost Optimization

## EMR Serverless vs Fixed Cluster

Fixed cluster (4 x m5.xlarge, 24/7): ~$500/month
EMR Serverless (scales to zero, runs only during batch): ~$85/month

**Savings: ~$415/month per pipeline**

Key config:
```python
spark.conf.set("spark.emr-serverless.executor.disk", "20G")
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "1")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
```

## Snowflake Warehouse Right-Sizing

Most pipelines run on X-SMALL warehouse. Only CFPB monthly report needs MEDIUM.

Auto-suspend config (highest ROI change):
```sql
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;
ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME = TRUE;
```

Leaving a warehouse running 24/7 vs auto-suspend at 60s:
- 24/7 at X-SMALL: ~$576/month
- Auto-suspend: ~$12-40/month depending on actual usage

## Micro-Batch Interval Tradeoff

10-second micro-batch vs 1-second:
- Snowflake write overhead: fixed cost per write, not per row
- 1s interval: 86,400 writes/day
- 10s interval: 8,640 writes/day — 10x fewer transactions
- Latency impact: 9 seconds additional end-to-end delay (acceptable for BI/reporting)

## S3 Raw Layer Retention

RAW data retained for 90 days in S3 Standard, then moved to S3 Glacier.
- Standard: $0.023/GB/month
- Glacier: $0.004/GB/month
- At 10TB/month ingestion, 90-day hot window = ~30TB → ~$700/month Standard
- Older data in Glacier: ~$120/month per 30TB

## Snowflake Storage vs Compute

Store data in S3, not Snowflake, for anything older than 90 days.
Snowflake storage: $40/TB/month
S3 Standard: $23/TB/month
S3 Glacier: $4/TB/month

Snowflake external tables over S3 Glacier for historical queries: zero storage cost in Snowflake.

## dbt Incremental Models

Full refresh of `fct_transactions` (3 years of data): ~45 minutes, ~120 credits.
Incremental with 3-day lookback: ~3 minutes, ~8 credits.

Config:
```sql
{{ config(materialized='incremental', unique_key='transaction_id',
          incremental_strategy='merge') }}

{% if is_incremental() %}
where event_timestamp >= dateadd('day', -3, current_timestamp())
{% endif %}
```

## Total Estimated Monthly Cost

| Component | Cost |
|-----------|------|
| EMR Serverless (streaming) | $85 |
| Snowflake compute | $200 |
| Snowflake storage (90-day hot) | $700 |
| S3 raw layer | $120 |
| MSK (Kafka) | $150 |
| **Total** | **~$1,255/month** |

At 100M events/day, cost per million events: **~$0.42**.

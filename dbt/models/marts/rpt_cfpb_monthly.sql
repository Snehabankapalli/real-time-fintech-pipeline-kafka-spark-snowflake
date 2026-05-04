{{
    config(materialized='table')
}}

-- CFPB-style monthly regulatory report.
-- Aggregated by event_timestamp (not processed_at) for accurate regulatory period attribution.

select
    event_month,
    count(*)                                                        as transaction_count,
    count(distinct account_token)                                   as unique_cardholders,
    sum(amount_usd)                                                 as total_volume_usd,
    sum(case when event_type = 'chargeback' then 1 else 0 end)      as chargeback_count,
    avg(case when event_type = 'chargeback' then 1.0 else 0.0 end)  as dispute_rate,
    sum(case when event_type = 'refund'     then amount_usd else 0 end) as total_refund_usd,
    avg(processing_latency_seconds)                                 as avg_latency_seconds
from {{ ref('fct_transactions') }}
group by 1
order by 1 desc

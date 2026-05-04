{{
    config(materialized='table')
}}

select
    current_date()                                              as report_date,
    count(*)                                                    as transactions_today,
    count(distinct account_token)                               as active_cardholders_today,
    sum(amount_usd)                                             as volume_today_usd,
    avg(processing_latency_seconds)                             as avg_latency_seconds,
    max(processing_latency_seconds)                             as max_latency_seconds,
    sum(case when is_dispute then 1 else 0 end)                 as disputes_today,
    avg(case when is_dispute then 1.0 else 0.0 end)             as dispute_rate_today
from {{ ref('fct_transactions') }}
where event_date = current_date()

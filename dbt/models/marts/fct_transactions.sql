{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        cluster_by=['event_date']
    )
}}

select
    transaction_id,
    account_token,
    event_type,
    amount_usd,
    currency,
    merchant_category,
    is_dispute,
    amount_tier,
    event_timestamp,
    event_date,
    event_month,
    processed_at,
    processing_latency_seconds
from {{ ref('int_transaction_enriched') }}

{% if is_incremental() %}
where event_timestamp >= dateadd('day', -3, current_timestamp())
{% endif %}

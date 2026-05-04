{{
    config(materialized='view')
}}

select
    transaction_id,
    account_token,
    event_type,
    cast(amount as decimal(18, 2))          as amount_usd,
    currency,
    coalesce(merchant_category, 'unknown')  as merchant_category,
    cast(event_timestamp as timestamp_ntz)  as event_timestamp,
    processed_at,
    datediff('second', event_timestamp, processed_at) as processing_latency_seconds
from {{ source('raw', 'card_transactions') }}
where transaction_id is not null
  and event_type in ('authorization', 'settlement', 'refund', 'chargeback')

-- Settlements must always have a positive amount.
-- A negative settlement indicates a data integrity issue upstream.

select transaction_id, amount_usd
from {{ ref('fct_transactions') }}
where event_type = 'settlement'
  and amount_usd < 0

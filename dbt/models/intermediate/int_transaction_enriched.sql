{{
    config(materialized='ephemeral')
}}

select
    stg.*,
    case
        when stg.event_type = 'chargeback' then true
        else false
    end                                          as is_dispute,
    case
        when stg.amount_usd >= 500  then 'high'
        when stg.amount_usd >= 100  then 'medium'
        else 'low'
    end                                          as amount_tier,
    date_trunc('day',  stg.event_timestamp)      as event_date,
    date_trunc('month', stg.event_timestamp)     as event_month
from {{ ref('stg_card_transactions') }} stg

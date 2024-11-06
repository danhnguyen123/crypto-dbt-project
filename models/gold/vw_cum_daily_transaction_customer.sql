{{ 
    config(
        materialized='view'
    ) 
}}

select customer_id, year, month, day_time, protocol_fee_amount
from {{ ref("cum_daily_transaction_customer") }}
order by customer_id, day_time
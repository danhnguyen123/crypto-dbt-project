{{ config(
    materialized='incremental',
    partition_by={
        "field": "day_time",       
        "data_type": "DATE"      
    },
    file_format='delta',
    unique_key='composite_key', 
    incremental_strategy='merge' 
) }}

-- Get upcoming daily transaction data (silver.transaction)
WITH new_data AS (
    SELECT
        txn_date AS day_time,
        customer_id,
        SUM(protocol_fee_amount) AS daily_protocol_fee
    FROM {{ source('silver','transaction') }}
    GROUP BY txn_date, customer_id
)
,
-- Get daily transaction by customer from existing table gold.daily_transaction_customer to calculate cumulatively
{% if is_incremental() %}
previous_data AS (
    SELECT
        day_time,
        customer_id,
        protocol_fee_amount
    FROM {{ this }}
)
{% else %}
-- If it' sfirst run, dummy data
previous_data AS (
    SELECT 
        null AS day_time, 
        null AS customer_id, 
        null AS protocol_fee_amount
    )
{% endif %}

-- Join upcoming and existing daily transaction data to calculate cumulatively 
,
merge_table as (
    SELECT 
    FROM (
        SELECT 
            coalesce(n.day_time, p.day_time) day_time,
            coalesce(n.customer_id, p.customer_id) customer_id,
            coalesce(n.protocol_fee_amount, p.protocol_fee_amount) protocol_fee_amount,
        FROM new_data AS n
        FULL JOIN previous_data AS p 
        ON n.day_time = p.day_time and n.customer_id = p.customer_id 
    )
    WHERE 1=1
    AND day_time is not null
    AND customer_id is not null
)
,
cumulate_free as (
select customer_id,
       day_time,
       --Calculate cumulate protocol_fee_amount
       SUM(protocol_fee_amount) over (partition by customer_id order by day_time) as protocol_fee_amount,
       {{ dbt_utils.surrogate_key(['customer_id', 'day_time']) }} AS composite_key
from cumulate_free
)

select *
from cumulate_free
{% if is_incremental() %}
-- Only process new or updated records for incremental runs
-- WHERE day_time >= date_format(date_add(current_date, -1), 'yyyy-MM-dd')
WHERE day_time >= DATEADD(day, -7, CURRENT_DATE)
{% endif %}
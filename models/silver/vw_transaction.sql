{{ 
    config(
        materialized='view',
        schema='silver'
    ) 
}}

select *
from {{ source('silver','transaction') }}
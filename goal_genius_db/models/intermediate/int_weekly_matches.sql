{{
    config(
        materialized = 'ephemeral'
    )
}}

select *
from {{ ref('stg_results') }} sr 
where date_time::date between 
    (date_trunc('week', current_date)::date) 
    and (date_trunc('week', current_date)::date + 6)

{{ config(
    unique_key =['league', 'date_key', 'home', 'away']
) }}

select *
from {{ source('raw','raw_results') }}

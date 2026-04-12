{{ config(
    unique_key='country_id'
) }}

select distinct
    country_id,
    country as country_name,
    flag_url,
    {{ dbt_date.now() }} as update_date
from {{ ref('country_flags') }}

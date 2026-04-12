-- models/dim_bookmakers.sql
{{ config(
    materialized='incremental',
    unique_key='bookmaker_id',
    incremental_strategy='merge'
) }}

{% if is_incremental() %}
with existing as (
    select
        bookmaker_id,
        bookmaker_name,
        /* keep a normalized version for matching */
        lower(trim(bookmaker_name)) as bookmaker_key
    from {{ this }}
),
{% else %}
with existing as (
    select
        null::int  as bookmaker_id,
        null::text as bookmaker_name,
        null::text as bookmaker_key
    where false
),
{% endif %}

-- Source distinct names from weekly odds
base_new as (
    select distinct
        trim(bookmaker_name) as bookmaker_name
    from {{ ref('stg_weekly_odds') }}
    where bookmaker_name is not null and trim(bookmaker_name) <> ''
),

-- Normalize for matching (case/whitespace)
new_bookmakers as (
    select
        bookmaker_name,
        lower(bookmaker_name) as bookmaker_key
    from base_new
),

-- Only names not already present
new_only as (
    select n.*
    from new_bookmakers n
    left join existing e
      on n.bookmaker_key = e.bookmaker_key
    where e.bookmaker_id is null
),

-- Assign new IDs after the current max
numbered_new as (
    select
        row_number() over (order by bookmaker_name)
        + coalesce((select max(bookmaker_id) from existing), 0) as bookmaker_id,
        bookmaker_name,
        lower(bookmaker_name) as bookmaker_key
    from new_only
)

select
    bookmaker_id,
    bookmaker_name,
    {{ dbt_date.now() }} as update_date
from (
    -- pass through existing rows untouched
    select
        e.bookmaker_id,
        e.bookmaker_name
    from existing e

    union all

    -- add the brand-new ones with fresh IDs
    select
        nn.bookmaker_id,
        nn.bookmaker_name
    from numbered_new nn
) final

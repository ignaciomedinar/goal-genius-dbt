{{ config(
    materialized='incremental',
    unique_key='league_id',
    incremental_strategy='merge'
) }}

{% if is_incremental() %}
with existing as (
    select league_id, 
           league_name, 
           country_id
    from {{ this }}
),
{% else %}
with existing as (
    select null::int  as league_id,
           null::text as league_name,
           null::int  as country_id
    where false
),
{% endif %}

-- single place to resolve latest country_id from leagues → dim_countries
latest_country as (
    select
        l.league_espn as league_name,
        c.country_id
    from {{ ref('leagues') }} l
    left join {{ ref('dim_countries') }} c
      on l.country = c.country_name
),

-- fill country_id for existing rows only when it's currently null
existing_fixed as (
    select
        e.league_id,
        e.league_name,
        coalesce(e.country_id, lc.country_id) as country_id
    from existing e
    left join latest_country lc
      on e.league_name = lc.league_name
),

new_leagues as (
    select distinct
        r.league as league_name,
        lc.country_id
    from {{ ref('stg_results') }} r
    left join latest_country lc
      on r.league = lc.league_name
),

new_only as (
    select n.*
    from new_leagues n
    left join existing e 
      on n.league_name = e.league_name
    where e.league_id is null
),

numbered_new as (
    select 
        row_number() over (order by league_name)
        + coalesce((select max(league_id) from existing), 0) as league_id,
        league_name,
        country_id
    from new_only
)

select
    league_id, 
    league_name, 
    country_id, 
    {{ dbt_date.now() }} as update_date
from (
    select * from existing_fixed
    union all
    select * from numbered_new
) final

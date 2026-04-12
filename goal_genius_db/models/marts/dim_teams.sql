{{ config(
    unique_key='team_id'
) }}

{% if is_incremental() %}
with existing as (
    select team_id, 
        team_name, 
        league_id
    from {{ this }}
),
{% else %}
with existing as (
    select null::int as team_id,
           null::text as team_name,
           null::int as league_id
    where false
),
{% endif %}

new_teams as (
    select distinct 
        team as team_name,
        dl.league_id
    from (
        select distinct home as team, 
            league 
        from {{ ref('stg_results') }}
        union all
        select distinct away as team, 
            league 
        from {{ ref('stg_results') }}
    ) as all_teams
    left join {{ ref('dim_leagues') }} dl 
    on all_teams.league = dl.league_name
),

-- Find only teams not already in the dimension
new_only as (
    select n.*
    from new_teams n
    left join existing e on n.team_name = e.team_name
    where e.team_id is null
),

-- Assign incremental IDs starting from max(existing team_id)
numbered_new as (
    select 
        row_number() over (order by team_name) 
        + coalesce((select max(team_id) from existing), 0) as team_id,
        team_name,
        league_id
    from new_only
)

select team_id, 
    team_name, 
    league_id, 
    {{ dbt_date.now() }} as update_date
from (
    select * from existing
    union all
    select * from numbered_new
) final

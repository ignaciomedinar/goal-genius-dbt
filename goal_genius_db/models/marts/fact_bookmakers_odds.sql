-- models/fact_bookmakers_odds.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['match_id','bookmaker_id']
) }}

with base as (
    select
        swo.*  -- bookmaker_name, league, home_team, away_team, match_date, home_odds, draw_odds, away_odds, etc.
    from {{ ref('stg_weekly_odds') }} swo
    {% if is_incremental() %}
      where swo.match_date::date >= (
        select coalesce(max(match_date)::date, date '2000-01-01')
        from {{ this }}
      )
    {% endif %}
),

-- map league to the canonical ESPN league name
league_map as (
    select
        l.league_odds,
        l.league_espn
    from {{ ref('leagues') }} l
),

-- enrich with canonical league & team display names for matching to dims
names as (
    select distinct
        b.*,
        lm.league_espn,
        tnh.team_name as home_dim,
        tna.team_name as away_dim
    from base b
    left join league_map lm
      on b.league = lm.league_odds
    left join {{ ref('team_names') }} tnh
      on b.home_team = tnh.odds_team_name
     and lm.league_espn = tnh.league_name
    left join {{ ref('team_names') }} tna
      on b.away_team = tna.odds_team_name
     and lm.league_espn = tna.league_name
),

-- join to dimension tables
joined as (
    select
        -- keys
        concat(
            to_char(n.match_date::date, 'YYYYMMDD'), '_',
            dl.league_id, '_',
            dth.team_id, '_',
            dta.team_id
        ) as match_id,

        n.match_date::date as match_date,
        cast(to_char(n.match_date::date, 'YYYYMMDD') as integer) as date_key,

        dl.league_id,
        dth.team_id as home_team_id,
        dta.team_id as away_team_id,

        db.bookmaker_id,

        -- measures
        n.home_odds::numeric as home_odds,
        n.draw_odds::numeric as draw_odds,
        n.away_odds::numeric as away_odds,

        -- useful lineage columns (optional)
        n.bookmaker_name,
        n.league,
        n.home_team,
        n.away_team,

        {{ dbt_date.now() }} as update_date
    from names n
    left join {{ ref('dim_leagues') }} dl
      on n.league_espn = dl.league_name
    left join {{ ref('dim_teams') }} dth
      on n.home_dim = dth.team_name
     and dth.league_id = dl.league_id
    left join {{ ref('dim_teams') }} dta
      on n.away_dim = dta.team_name
     and dta.league_id = dl.league_id
    left join {{ ref('dim_bookmakers') }} db
      on lower(trim(n.bookmaker_name)) = lower(trim(db.bookmaker_name))
),

-- keep only well-formed fact rows (all FK’s present)
filtered as (
    select *
    from joined
    where league_id is not null
      and home_team_id is not null
      and away_team_id is not null
      and bookmaker_id is not null
)

select * from filtered

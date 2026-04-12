{{ config(
    materialized='incremental',
    unique_key=['league_id', 'date_key', 'home_team_id', 'away_team_id'],
    on_schema_change='sync'
) }}

select concat(sr.date_key, '_', dl.league_id, '_', dth.team_id, '_', dta.team_id) AS match_id,
    dl.league_id,
    sr.date_key,
    sr.date_time,
    dth.team_id as home_team_id,
    dta.team_id as away_team_id,
    sr.goals_home,
    sr.goals_away,
    fmr.pag,
    fmr.phg,
    fmr.bet_id,
    fmr.max_prob,
    fmr.liability_id,
    db.bet_id as actual_result_id,
    case 
        when db.bet_id = 1 and fmr.bet_id in (1, 4) then 1
        when db.bet_id = 2 and fmr.bet_id in (2, 5) then 1
        when db.bet_id = 3 and fmr.bet_id in (3, 4, 5) then 1
        when fmr.bet_id is null then null
        when db.bet_id is null then null
        else 0
    end as prediction_is_correct,
    NOW() AS created_at
from {{ ref('stg_results') }} sr
left join {{ ref('dim_leagues') }} dl
on sr.league = dl.league_name
left join {{ ref('dim_teams') }} dth
on sr.home = dth.team_name
and dth.league_id = dl.league_id
left join {{ ref('dim_teams') }} dta
on sr.away = dta.team_name
and dta.league_id = dl.league_id
left join {{ ref('fact_match_predictions') }} fmr
on dl.league_id = fmr.league_id
and sr.date_key = fmr.date_key
and dth.team_id = fmr.home_team_id
and dta.team_id = fmr.away_team_id
left join {{ ref('dim_bet') }} db
on case
        when sr.goals_home > sr.goals_away then 'home'
        when sr.goals_home < sr.goals_away then 'away'
        when sr.goals_home = sr.goals_away then 'draw'
        else null
    end = db.bet_name
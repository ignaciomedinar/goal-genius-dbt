{{ config(
    materialized='incremental',
    unique_key=['league_id', 'date_key', 'home_team_id', 'away_team_id'],
    on_schema_change='sync'
) }}

with df_final AS (
    SELECT
        concat(date_key, '_', dl.league_id, '_', dth.team_id, '_', dta.team_id) AS match_id,
        dl.league_id,
        date_time,
        date_key,
        dth.team_id AS home_team_id,
        dta.team_id AS away_team_id,
        max_prob,
        db.bet_id,
        ROUND(phg::numeric,0) AS phg,
        ROUND(pag::numeric,0) AS pag,
        dtl.liability_id,
        NOW() AS created_at
    FROM {{ ref('int_max_probs')}} imp
    left join {{ ref('dim_leagues') }} dl
    on imp.league = dl.league_name
    left join {{ ref('dim_teams') }} dth
    on imp.home = dth.team_name
    and dth.league_id = dl.league_id
    left join {{ ref('dim_teams') }} dta
    on imp.away = dta.team_name
    and dta.league_id = dl.league_id
    left join {{ ref('int_team_liability') }} talh
    on dth.team_id = talh.team_id
    and dl.league_id = talh.league_id
    left join {{ ref('int_team_liability') }} tala
    on dta.team_id = tala.team_id
    and dl.league_id = tala.league_id
    left join {{ ref('dim_bet') }} db
    on (CASE 
            WHEN phg = pag AND imp.bet = 'home' THEN 'draw->home'
            WHEN phg = pag AND imp.bet = 'away' THEN 'draw->away'
            WHEN phg = pag AND imp.bet = 'draw' THEN 'draw'
            ELSE imp.bet
        END) = db.bet_name
    left join {{ ref('dim_team_liability') }} dtl
    on (CASE 
            WHEN talh.home_liability = 'high' and tala.away_liability = 'high' THEN 3
            WHEN imp.max_prob >= .85 and talh.home_liability = 'mid' and tala.away_liability = 'mid' THEN 3
            WHEN talh.home_liability = 'mid' and tala.away_liability = 'mid' THEN 2
            ELSE 1
        END) = dtl.liability_id
    where max_prob IS NOT NULL
)
SELECT distinct *
FROM df_final df
order by liability_id desc,
    max_prob desc

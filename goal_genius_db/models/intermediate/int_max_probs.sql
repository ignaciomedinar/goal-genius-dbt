{{
    config(
        materialized = 'ephemeral'
    )
}}

SELECT
    *,
    GREATEST(home_win, draw, away_win) AS max_prob,
    CASE 
        WHEN GREATEST(home_win, draw, away_win) = home_win THEN 'home'
        WHEN GREATEST(home_win, draw, away_win) = away_win THEN 'away'
        WHEN GREATEST(home_win, draw, away_win) = draw THEN 'draw'
    END AS bet
FROM {{ ref('int_match_outcomes')}}

SELECT
    league,
    date_time,
    date_key,
    home,
    away,
    phg,
    pag,
    SUM(CASE WHEN home_goals_aux > away_goals_aux THEN prob ELSE 0 END) AS home_win,
    SUM(CASE WHEN home_goals_aux = away_goals_aux THEN prob ELSE 0 END) AS draw,
    SUM(CASE WHEN home_goals_aux < away_goals_aux THEN prob ELSE 0 END) AS away_win
FROM {{ ref('int_match_probabilities')}}
GROUP BY league,
    date_time,
    date_key,
    home,
    away,
    phg,
    pag

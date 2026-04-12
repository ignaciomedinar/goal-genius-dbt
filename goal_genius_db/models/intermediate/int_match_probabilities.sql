{{
    config(
        materialized = 'table'
    )
}}

with prob_base as (
select *,
	attack_home * defense_away * avg_g_scored_per_league_h as phg,
	attack_away * defense_home * avg_g_scored_per_league_a as pag,
	(attack_home * defense_away * avg_g_scored_per_league_h) + (attack_away * defense_home * avg_g_scored_per_league_a) as ptg
from {{ ref('int_team_attack_defense')}} ms
),
prob_goals as (
select league,
	date_time,
    date_key,
	home,
	away,
	phg,
	pag,
	(POWER(pag, 0) * EXP(-pag)) / FACTORIAL(0) AS p_0ag,
    (POWER(pag, 1) * EXP(-pag)) / FACTORIAL(1) AS p_1ag,
    (POWER(pag, 2) * EXP(-pag)) / FACTORIAL(2) AS p_2ag,
    (POWER(pag, 3) * EXP(-pag)) / FACTORIAL(3) AS p_3ag,
    (POWER(pag, 4) * EXP(-pag)) / FACTORIAL(4) AS p_4ag,
    (POWER(pag, 5) * EXP(-pag)) / FACTORIAL(5) AS p_5ag,
    (POWER(pag, 6) * EXP(-pag)) / FACTORIAL(6) AS p_6ag,
    (POWER(pag, 7) * EXP(-pag)) / FACTORIAL(7) AS p_7ag,
    (POWER(phg, 0) * EXP(-phg)) / FACTORIAL(0) AS p_0hg,
    (POWER(phg, 1) * EXP(-phg)) / FACTORIAL(1) AS p_1hg,
    (POWER(phg, 2) * EXP(-phg)) / FACTORIAL(2) AS p_2hg,
    (POWER(phg, 3) * EXP(-phg)) / FACTORIAL(3) AS p_3hg,
    (POWER(phg, 4) * EXP(-phg)) / FACTORIAL(4) AS p_4hg,
    (POWER(phg, 5) * EXP(-phg)) / FACTORIAL(5) AS p_5hg,
    (POWER(phg, 6) * EXP(-phg)) / FACTORIAL(6) AS p_6hg,
    (POWER(phg, 7) * EXP(-phg)) / FACTORIAL(7) AS p_7hg
    from prob_base
 ),
 prob_scorelines AS (
    SELECT
        g.league,
        g.date_time,
        g.date_key,
        g.home,
        g.away,
        g.phg,
        g.pag,
        i AS home_goals_aux,
        j AS away_goals_aux,
        (CASE i
            WHEN 0 THEN g.p_0hg
            WHEN 1 THEN g.p_1hg
            WHEN 2 THEN g.p_2hg
            WHEN 3 THEN g.p_3hg
            WHEN 4 THEN g.p_4hg
            WHEN 5 THEN g.p_5hg
            WHEN 6 THEN g.p_6hg
            WHEN 7 THEN g.p_7hg
         END) *
        (CASE j
            WHEN 0 THEN g.p_0ag
            WHEN 1 THEN g.p_1ag
            WHEN 2 THEN g.p_2ag
            WHEN 3 THEN g.p_3ag
            WHEN 4 THEN g.p_4ag
            WHEN 5 THEN g.p_5ag
            WHEN 6 THEN g.p_6ag
            WHEN 7 THEN g.p_7ag
         END) AS prob
    FROM prob_goals g
    CROSS JOIN generate_series(0,7) i
    CROSS JOIN generate_series(0,7) j
 )
 select *
 from prob_scorelines

{{
    config(
        materialized = 'ephemeral'
    )
}}

select league,
	home_away,
	AVG(goals_scored_per_game) as avg_g_scored_per_league,
	AVG(goals_received_per_game) as avg_g_received_per_league
from {{ ref('int_team_scores') }} 
group by 1,2

with team_attack_defense as (
select ts.league,
	ts.team,
	ts.home_away,
	ls.avg_g_scored_per_league,
	ls.avg_g_received_per_league,
	case when ts.goals_scored_per_game>0 then ts.goals_scored_per_game/ls.avg_g_scored_per_league
		when ts.goals_scored_per_game <= 0 then ls.avg_g_scored_per_league end as attack,
	case when ts.goals_received_per_game>0 then ts.goals_received_per_game/ls.avg_g_received_per_league 
		when ts.goals_received_per_game <= 0 then ls.avg_g_received_per_league end as defense
from {{ ref('int_team_scores')}} ts
left join {{ ref('int_league_scores')}} ls
on ts.league = ls.league 
and ts.home_away = ls.home_away 
)
select wm.*,
	ta.avg_g_scored_per_league as avg_g_scored_per_league_h,
	ta.avg_g_received_per_league as avg_g_received_per_league_h,
	td.avg_g_scored_per_league as avg_g_scored_per_league_a,
	td.avg_g_received_per_league as avg_g_received_per_league_a,
	ta.attack as attack_home,
	ta.defense as defense_home,
	td.attack as attack_away,
	td.defense as defense_away
from {{ ref('int_weekly_matches')}} wm
left join team_attack_defense ta
on wm.league = ta.league
and ta.home_away = 'home'
and wm.home = ta.team
left join team_attack_defense td
on wm.league = td.league
and td.home_away = 'away'
and wm.away = td.team
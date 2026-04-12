select wm.home as team,
	wm.league,
	'home' as home_away,
	count(*) as matches,
	sum(coalesce(sr.goals_home,0)) as goals_scored,
	sum(coalesce(sr.goals_away,0)) as goals_received,
	sum(coalesce(sr.goals_home,0))/count(*) as goals_scored_per_game,
	sum(coalesce(sr.goals_away,0))/count(*) as goals_received_per_game
from {{ ref('int_weekly_matches')}} wm
left join {{ ref('stg_results') }} sr 
on wm.home = sr.home 
and wm.league = sr.league
and sr.goals_home is not null
and sr.date_time::date between
      (date_trunc('week', {{ dbt_date.today() }}) - interval '1 day' - interval '548 days') 
  and (date_trunc('week', {{ dbt_date.today() }}) - interval '1 day')
group by 1,2,3
union all
select wm.away as team,
	wm.league,
	'away' as home_away,
	count(*) as matches,
	sum(coalesce(sr.goals_away,0)) as goals_scored,
	sum(coalesce(sr.goals_home,0)) as goals_received,
	sum(coalesce(sr.goals_away,0))/count(*) as goals_scored_per_game,
	sum(coalesce(sr.goals_home,0))/count(*) as goals_received_per_game
from {{ ref('int_weekly_matches')}} wm
left join {{ ref('stg_results') }} sr 
on wm.away = sr.away 
and wm.league = sr.league
and sr.goals_away is not null
and sr.date_time::date between
      (date_trunc('week', {{ dbt_date.today() }}) - interval '1 day' - interval '548 days') 
  and (date_trunc('week', {{ dbt_date.today() }}) - interval '1 day')
group by 1,2,3
with games_played as (
select dt.team_id,
	dt.league_id,
	count(distinct srh.date_time::date) as home_games,
	count(distinct sra.date_time::date) as away_games,
	case when count(distinct srh.date_time::date) >20 then 'high'
		when count(distinct srh.date_time::date) between 11 and 20 then 'mid'
		else 'low' end as home_liability,
	case when count(distinct sra.date_time::date) >20 then 'high'
		when count(distinct sra.date_time::date) between 11 and 20 then 'mid'
		else 'low' end as away_liability
from {{ ref('dim_teams')}} dt 
left join {{ ref('dim_leagues')}} dl 
on dt.league_id = dl.league_id 
left join {{ ref('stg_results') }} srh
on dl.league_name  = srh.league 
and dt.team_name = srh.home 
and srh.date_time::date between
      (date_trunc('week', {{ dbt_date.today() }}) - interval '1 day' - interval '548 days') 
  and (date_trunc('week', {{ dbt_date.today() }}) - interval '1 day')
and srh.match_status = 'Full Time'
left join {{ ref('stg_results') }} sra
on dl.league_name  = sra.league 
and dt.team_name = sra.away 
and sra.date_time::date between
      (date_trunc('week', {{ dbt_date.today() }}) - interval '1 day' - interval '548 days') 
  and (date_trunc('week', {{ dbt_date.today() }}) - interval '1 day')
and sra.match_status = 'Full Time'
group by dt.team_id,
	dt.league_id
)
select *
from games_played

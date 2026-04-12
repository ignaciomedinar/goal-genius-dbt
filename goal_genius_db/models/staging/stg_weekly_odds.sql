{{ config(
    unique_key =['league', 'home_team', 'away_team', 'match_date', 'bookmaker_name']
) }}

select 
    "League" as league,
    "Home" as home_team,
    "Away" as away_team,
    "Date" as match_date,
    "Bookmaker" as bookmaker_name,
    "HomeOdds" as home_odds,
    "DrawOdds" as draw_odds,
    "AwayOdds" as away_odds,
    {{ dbt_date.now() }} as update_date
from {{ source('raw','raw_weekly_odds') }}

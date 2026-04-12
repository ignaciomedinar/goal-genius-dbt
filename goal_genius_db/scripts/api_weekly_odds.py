import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv() 
API_KEY = os.getenv("API_KEY")
# SPORT = 'soccer_mexico_ligamx'
REGIONS = 'us'
MARKETS = 'h2h'
ODDS_FORMAT = 'decimal'
DATE_FORMAT = 'iso'

# Fetch all available sports/leagues
response = requests.get(
    'https://api.the-odds-api.com/v4/sports/',
    params={'api_key': API_KEY}
)

if response.status_code == 200:
    sports = response.json()
    leagues = [sport['key'] for sport in sports if sport['group'].lower().startswith('soccer')]
else:
    print(f"Failed to fetch sports: {response.status_code}, {response.text}")
    leagues = []
    
pg_url = os.getenv("PG_URL")
engine = create_engine(pg_url)  

# Drop the table if it exists at the beginning
with engine.connect() as connection:
    query = text("DROP TABLE IF EXISTS raw.raw_weekly_odds")
    connection.execute(query)

for SPORT in leagues:
    # Request the odds data from the API
    odds_response = requests.get(
        f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds',
        params={
            'api_key': API_KEY,
            'regions': REGIONS,
            'markets': MARKETS,
            'oddsFormat': ODDS_FORMAT,
            'dateFormat': DATE_FORMAT,
        }
    )

    if odds_response.status_code != 200:
        print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')
    else:
        odds_json = odds_response.json()
        
        # Extract relevant data
        data = []
        for event in odds_json:
            # Extract basic event information
            league=event['sport_title']
            away_team = event['away_team']
            date = event['commence_time'][:10]  # Extract only the date part (YYYY-MM-DD)
            home_team = event['home_team']

            # Loop through all bookmakers
            for bookmaker in event['bookmakers']:
                # Use 'title' if available, otherwise use 'key'
                bookmaker_name = bookmaker.get('title', bookmaker['key'])
                markets = bookmaker['markets']
                
                # Extract prices from the 'h2h' market
                h2h_market = next((market for market in markets if market['key'] == 'h2h'), None)
                if h2h_market and 'outcomes' in h2h_market:
                    outcomes = h2h_market['outcomes']
                    home_odds = next((outcome['price'] for outcome in outcomes if outcome['name'] == home_team), None)
                    draw_odds = next((outcome['price'] for outcome in outcomes if outcome['name'] == 'Draw'), None)
                    away_odds = next((outcome['price'] for outcome in outcomes if outcome['name'] == away_team), None)

                    # Append the row data
                    data.append({
                        'League': league,
                        'Home': home_team,
                        'Away': away_team,
                        'Date': date,
                        'Bookmaker': bookmaker_name,
                        'HomeOdds': home_odds,
                        'DrawOdds': draw_odds,
                        'AwayOdds': away_odds
                    })

        if data:
            df = pd.DataFrame(data)
            # Append to the SQL table
            df.to_sql('raw_weekly_odds', schema = 'raw', con=engine, if_exists='append', index=False)
            print(f"Data from {SPORT} league inserted into the database.")
            
engine.dispose()
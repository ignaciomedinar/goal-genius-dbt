import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os

load_dotenv()  # read .env file
pg_url = os.getenv("PG_URL")
pg_engine = create_engine(pg_url)

def get_driver():
    options = Options()
    options.add_argument("--headless=new")  # headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scrape_day(date: datetime):
    """Scrape one day of matches from ESPN scoreboard."""
    url_date = date.strftime("%Y%m%d")
    url = f"https://www.espn.com/soccer/scoreboard/_/date/{url_date}"

    driver = get_driver()
    driver.get(url)

    sections = driver.find_elements(By.CSS_SELECTOR, "section.Card.gameModules")
    matches = []

    for sec in sections:
        try:
            league = sec.find_element(By.CSS_SELECTOR, "h3.Card__Header__Title").text
        except:
            league = None

        try:
            teams = [el.text for el in sec.find_elements(By.CSS_SELECTOR, "div.ScoreCell__TeamName")]
        except:
            teams = []
        try:
            scores = [el.text for el in sec.find_elements(By.CSS_SELECTOR, "div.ScoreCell__Score")]
        except:
            scores = []

        try:
            time_elems = sec.find_elements(By.CSS_SELECTOR, "div.ScoreCell__Time")
        except:
            time_elems = []

        for i in range(0, len(teams), 2):
            status_text = time_elems[i // 2].text if i // 2 < len(time_elems) else ""

            # Determine match_status and date_time
            if status_text == "FT" or status_text == "FT-Pens":
                match_status = "Full Time"
                dt_cet = datetime.strptime(f"{date.date()} 00:00", "%Y-%m-%d %H:%M").replace(
                    tzinfo=ZoneInfo("Europe/Madrid")
                )
            elif status_text.lower() == "postponed":
                match_status = "Postponed"
                dt_cet = datetime.strptime(f"{date.date()} 00:00", "%Y-%m-%d %H:%M").replace(
                    tzinfo=ZoneInfo("Europe/Madrid")
                )
            elif status_text.lower() == "canceled":
                match_status = "Canceled"
                dt_cet = datetime.strptime(f"{date.date()} 00:00", "%Y-%m-%d %H:%M").replace(
                    tzinfo=ZoneInfo("Europe/Madrid")
                )
            elif date.date() >= datetime.now().date():
                match_status = "Upcoming"
                try:
                    dt_cet = datetime.strptime(f"{date.date()} {status_text}", "%Y-%m-%d %I:%M %p").replace(
                        tzinfo=ZoneInfo("Europe/Madrid")
                    )
                except:
                    dt_cet = datetime.strptime(f"{date.date()} 00:00", "%Y-%m-%d %H:%M").replace(
                        tzinfo=ZoneInfo("Europe/Madrid")
                    )
            else:
                match_status = "Other"
                dt_cet = datetime.strptime(f"{date.date()} 00:00", "%Y-%m-%d %H:%M").replace(
                    tzinfo=ZoneInfo("Europe/Madrid")
                )

        # group into matches (home, away)
        for i in range(0, len(teams), 2):
            match = {
                "league": league,
                "date_time": dt_cet,
                "home": teams[i],
                "away": teams[i+1] if i+1 < len(teams) else None,
                "goals_home": int(scores[i]) if i < len(scores) and scores[i].isdigit() else None,
                "goals_away": int(scores[i+1]) if i+1 < len(scores) and scores[i+1].isdigit() else None,
                "match_status": match_status,
            }
            matches.append(match)

    driver.quit()
    print(date, ": completed", " | matches: ", len(matches))
    return matches


def update_raw_results():
    today = datetime.now().date()

    # ---- Ensure schema + table exist ----
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw.raw_results (
        league TEXT,
        date_time TIMESTAMPTZ,
        date_key BIGINT,
        home TEXT,
        away TEXT,
        goals_home INT,
        goals_away INT,
        match_status TEXT,
        update_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (league, date_key, home, away)
    );
    """
    with pg_engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text(create_table_sql))

    # ---- Find last date with missing scores ----
    query_missing = """
    select (max(update_date)-'2 days'::interval)::date as last_update
    from raw.raw_results;
    """
    with pg_engine.connect() as conn:
        result = conn.execute(text(query_missing)).scalar()

    if result:
        # last_missing_date = result
        # start_date = last_missing_date
        start_date = result # - timedelta(days=1)
        # start_date = datetime(2025, 9, 16, tzinfo=None)
    else:
        # fallback if no missing scores
        start_date = today - timedelta(days=15)

    # start_date = today - timedelta(days=2) # override for testing
    end_date = today + timedelta(days=7)

    print(f"Scraping window: {start_date} -> {end_date}")

    # ---- Scrape matches ----
    all_matches = []
    for d in pd.date_range(start_date, end_date):
        all_matches.extend(scrape_day(d))

    df = pd.DataFrame(all_matches)
    df["date_time"] = df["date_time"].dt.tz_localize(None)
    df["date_key"] = df["date_time"].dt.strftime("%Y%m%d").astype(int)
    df = df.replace({np.nan: None})

    # ---- UPSERT into Postgres ----
    upsert_sql = """
    INSERT INTO raw.raw_results (league, date_time, date_key, home, away, goals_home, goals_away, match_status)
    VALUES (:league, :date_time, :date_key, :home, :away, :goals_home, :goals_away, :match_status)
    ON CONFLICT (league, date_key, home, away)
    DO UPDATE SET
        goals_home = CASE
            WHEN EXCLUDED.goals_home IS NOT NULL THEN EXCLUDED.goals_home
            ELSE raw.raw_results.goals_home
        END,
        goals_away = CASE
            WHEN EXCLUDED.goals_away IS NOT NULL THEN EXCLUDED.goals_away
            ELSE raw.raw_results.goals_away
        END,
        match_status = EXCLUDED.match_status,
        date_time = CASE
            WHEN raw.raw_results.date_time IS NOT NULL THEN raw.raw_results.date_time
            ELSE EXCLUDED.date_time
        END,
        update_date = CASE
            WHEN raw.raw_results.goals_home IS DISTINCT FROM EXCLUDED.goals_home
            OR raw.raw_results.goals_away IS DISTINCT FROM EXCLUDED.goals_away
            THEN CURRENT_TIMESTAMP
            ELSE raw.raw_results.update_date
        END;
    """

    # upsert_sql = upsert_sql.where(pd.notnull(upsert_sql), None)
    with pg_engine.begin() as conn:
        conn.execute(text(upsert_sql), df.to_dict(orient="records"))

    print("✅ Upsert completed into raw.raw_results")

    return df


# Run update
df_matches = update_raw_results()
print("Total matches processed: ", len(df_matches))

# Optional: export to CSV
# df_matches.to_csv("matches_scraped.csv", index=False)
# print("CSV exported: matches_scraped.csv")

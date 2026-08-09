import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
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

def _parse_espn_utc(date_str: str) -> datetime:
    """Parse ESPN's ISO-8601 UTC event timestamp, e.g. '2026-08-09T22:30Z'."""
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def scrape_day(date: datetime):
    """Scrape one day of matches from ESPN scoreboard.

    Reads the event data straight out of ESPN's embedded page state
    (window['__espnfitt__']) instead of the rendered DOM text. The DOM only
    exposes a locale-formatted time (e.g. "12:30 AM") with no timezone
    attached, and ESPN buckets matches into scoreboard "days" using its own
    internal reference timezone (not Europe/Madrid) -- so a late-night match
    can render on a date page that no longer matches its real Madrid-local
    calendar day. The embedded JSON instead carries an unambiguous UTC
    instant per event, which we convert to Europe/Madrid ourselves.
    """
    url_date = date.strftime("%Y%m%d")
    url = f"https://www.espn.com/soccer/scoreboard/_/date/{url_date}"

    driver = get_driver()
    driver.get(url)

    try:
        # ESPN's scoreboard renders match cards client-side after the initial
        # page load, so give the JS a chance to populate them before reading.
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "section.Card.gameModules"))
        )
    except TimeoutException:
        pass  # genuinely no matches scheduled that day

    try:
        fitt = driver.execute_script("return window['__espnfitt__']")
        league_groups = fitt["page"]["content"]["scoreboard"]["gmsByLeague"]
    except Exception:
        league_groups = []

    matches = []

    for group in league_groups:
        league = (group.get("league") or {}).get("name")

        for evt in group.get("evts", []):
            competitors = evt.get("competitors") or []
            home = next((c for c in competitors if c.get("isHome")), None)
            away = next((c for c in competitors if not c.get("isHome")), None)
            if home is None or away is None:
                continue  # incomplete event (e.g. a bye), nothing usable to store

            status = evt.get("status") or {}
            state = status.get("state")  # 'pre' | 'in' | 'post'
            detail = (status.get("detail") or status.get("description") or "").lower()

            if "postpon" in detail:
                match_status = "Postponed"
            elif "cancel" in detail:
                match_status = "Canceled"
            elif state == "post":
                match_status = "Full Time"
            elif state == "pre":
                match_status = "Upcoming"
            else:
                match_status = "Other"

            date_str = evt.get("date")
            if date_str:
                dt_cet = _parse_espn_utc(date_str).astimezone(ZoneInfo("Europe/Madrid"))
            else:
                dt_cet = datetime.strptime(f"{date.date()} 00:00", "%Y-%m-%d %H:%M").replace(
                    tzinfo=ZoneInfo("Europe/Madrid")
                )

            def _goals(competitor):
                score = competitor.get("score")
                return int(score) if score is not None and str(score).isdigit() else None

            match = {
                "league": league,
                "date_time": dt_cet,
                "home": home.get("displayName"),
                "away": away.get("displayName"),
                "goals_home": _goals(home),
                "goals_away": _goals(away),
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
    if df.empty:
        raise RuntimeError(
            f"Scraped 0 matches across the entire window {start_date} -> {end_date}; "
            "ESPN page likely failed to render or is blocking the scraper. Aborting before upsert."
        )
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

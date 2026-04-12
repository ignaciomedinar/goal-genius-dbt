import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
pg_url = os.getenv("PG_URL")
pg_engine = create_engine(pg_url)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

def load_csv_to_staging(filename, table_name):
    file_path = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(file_path)

    with pg_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS staging"))
        # Replace table with fresh data each run
        conn.execute(text(f"DROP TABLE IF EXISTS staging.{table_name} CASCADE"))
        df.to_sql(table_name, con=conn, schema="staging", if_exists="replace", index=False)

    print(f"✅ Loaded {filename} into staging.{table_name} (rows: {len(df)})")

if __name__ == "__main__":
    load_csv_to_staging("country_flags.csv", "raw_flags")
    load_csv_to_staging("teams.csv", "raw_teams")
    load_csv_to_staging("leagues.csv", "raw_leagues")
    load_csv_to_staging("team_aliases.csv", "raw_team_aliases")

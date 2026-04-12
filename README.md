# Goal Genius

A data pipeline and analytics project that collects football (soccer) match results and betting odds, models them with dbt, and produces match outcome predictions using the Dixon-Coles / Poisson scoring method.

---

## Architecture

```
Data Sources
  ├── ESPN Scoreboard (scraper)        → raw.raw_results
  └── The Odds API                     → raw.raw_weekly_odds

dbt Pipeline
  ├── staging/     – light cleaning, source references
  ├── intermediate/ – attack/defense ratings, Poisson probabilities
  └── marts/       – dimensions & facts ready for consumption
```

### Data flow

| Layer | Models | Description |
|---|---|---|
| **Raw** | `raw_results`, `raw_weekly_odds` | Ingested as-is from scripts |
| **Staging** | `stg_results`, `stg_weekly_odds` | Typed, deduplicated |
| **Intermediate** | `int_team_scores`, `int_league_scores`, `int_team_attack_defense`, `int_match_probabilities`, `int_max_probs`, `int_weekly_matches`, `int_match_outcomes`, `int_team_liability` | Feature engineering & probability modelling |
| **Marts** | `fact_match_predictions`, `fact_match_results`, `fact_bookmakers_odds`, `dim_leagues`, `dim_teams`, `dim_countries`, `dim_bookmakers`, `dim_bet`, `dim_team_liability` | Analytics-ready |

---

## Prediction Model

Match outcome probabilities are computed using the **Poisson distribution**:

1. Per-team attack and defense ratings are derived from historical goals scored/conceded relative to league averages.
2. Expected goals (λ) for each side are calculated as:
   - `λ_home = attack_home × defense_away × avg_league_goals_home`
   - `λ_away = attack_away × defense_home × avg_league_goals_away`
3. Scoreline probabilities are computed for all combinations up to 7 goals per side.
4. Home win, draw, and away win probabilities are aggregated from scoreline probabilities.
5. The highest-probability outcome is surfaced in `fact_match_predictions`, ranked by liability tier and probability.

---

## Project Structure

```
goal-genius/
├── .env                        # Local secrets (not committed)
├── .envrc                      # direnv config – loads .env automatically
├── .gitignore
└── goal_genius_db/             # dbt project
    ├── dbt_project.yml
    ├── packages.yml
    ├── models/
    │   ├── raw/
    │   ├── staging/
    │   ├── intermediate/
    │   └── marts/
    ├── seeds/                  # Static reference data (leagues, teams, flags)
    ├── scripts/                # Python ingestion scripts
    │   ├── scrape_results.py   # Scrapes ESPN for match results
    │   ├── api_weekly_odds.py  # Pulls odds from The Odds API
    │   └── csvs_loading.py     # Loads reference CSVs into staging
    ├── macros/
    ├── tests/
    └── analyses/
```

---

## Prerequisites

| Tool | Purpose |
|---|---|
| Python 3.12+ | Running ingestion scripts |
| dbt-postgres | Running the dbt pipeline |
| PostgreSQL | Target database (hosted on Railway or local) |
| [direnv](https://direnv.net/) | Auto-loading `.env` (optional but recommended) |

---

## Setup

### 1. Environment variables

Create a `.env` file in the project root:

```env
PG_URL=postgresql://<user>:<password>@<host>:<port>/<database>
API_KEY=<your_the_odds_api_key>
```

> `.env` is git-ignored. Never commit it.

### 2. Python dependencies

```bash
python -m venv .v
source .v/bin/activate        # Windows: .v\Scripts\activate
pip install dbt-postgres python-dotenv sqlalchemy pandas requests selenium webdriver-manager
```

### 3. dbt profile

Add a profile named `goal_genius_db` to `~/.dbt/profiles.yml`:

```yaml
goal_genius_db:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('PGHOST') }}"
      user: "{{ env_var('PGUSER') }}"
      password: "{{ env_var('PGPASSWORD') }}"
      port: 5432
      dbname: railway
      schema: public
      threads: 4
```

Or point directly to `PG_URL` depending on your setup.

### 4. Install dbt packages

```bash
cd goal_genius_db
dbt deps
```

---

## Running the Pipeline

### Ingest raw data

```bash
# Scrape match results from ESPN
python goal_genius_db/scripts/scrape_results.py

# Pull weekly betting odds from The Odds API
python goal_genius_db/scripts/api_weekly_odds.py

# Load reference CSVs (leagues, teams, flags)
python goal_genius_db/scripts/csvs_loading.py
```

### Run dbt models

```bash
cd goal_genius_db

# Full run
dbt run

# Run only marts
dbt run --select marts

# Run with tests
dbt build
```

---

## dbt Packages

| Package | Version |
|---|---|
| `dbt-labs/dbt_utils` | 1.3.1 |
| `calogica/dbt_date` | 0.10.1 |

---

## Key Output: `fact_match_predictions`

The primary output table. Each row represents an upcoming match with:

| Column | Description |
|---|---|
| `match_id` | Surrogate key |
| `league_id` | FK to `dim_leagues` |
| `date_time` | Kick-off datetime (CET) |
| `home_team_id` / `away_team_id` | FK to `dim_teams` |
| `max_prob` | Probability of the predicted outcome |
| `bet_id` | Predicted outcome (home / draw / away) |
| `phg` / `pag` | Expected goals home / away |
| `liability_id` | Confidence tier (1 = low, 3 = high) |

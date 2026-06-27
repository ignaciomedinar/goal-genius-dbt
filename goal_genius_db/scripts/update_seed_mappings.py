"""
Reconciles seeds/leagues.csv and seeds/team_names.csv against the league and
team names that were just fetched into raw.raw_weekly_odds.

fact_bookmakers_odds joins odds rows to dim_leagues/dim_teams through these
two seeds. Any odds-side league/team name with no row here gets silently
dropped from that join. This script closes that gap by:
  - filling in the odds-side column (league_odds / odds_team_name) on an
    existing seed row when a confident fuzzy match is found, or
  - appending a new row with the canonical (ESPN) side left blank, so it
    shows up for manual completion instead of disappearing.
"""
import csv
import difflib
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
pg_engine = create_engine(os.getenv("PG_URL"))

SEEDS_DIR = os.path.join(os.path.dirname(__file__), "..", "seeds")
LEAGUES_PATH = os.path.join(SEEDS_DIR, "leagues.csv")
TEAMS_PATH = os.path.join(SEEDS_DIR, "team_names.csv")

# Leagues are matched against ALL other leagues (cross-league name collisions
# are easy, e.g. "FIFA World Cup" vs "FIFA Club World Cup"), so this needs to
# be strict. Teams are matched within a single already-resolved league, where
# look-alikes are rare, so it can be looser to catch things like
# "Athletic Bilbao" vs "Athletic Club".
LEAGUE_MATCH_THRESHOLD = 0.90
TEAM_MATCH_THRESHOLD = 0.72


def read_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames), list(reader)


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(rows)


def fuzzy_best(name, choices, threshold):
    lower_map = {c.lower(): c for c in choices}
    found = difflib.get_close_matches(name.lower(), lower_map.keys(), n=1, cutoff=threshold)
    return lower_map[found[0]] if found else None


def fetch_odds_entries():
    with pg_engine.connect() as conn:
        return conn.execute(text(
            'select distinct "League" as league, "Home" as home, "Away" as away '
            'from raw.raw_weekly_odds'
        )).mappings().all()


def reconcile_leagues(rows, odds_leagues):
    existing_odds = {r["league_odds"].strip() for r in rows if r["league_odds"].strip()}
    max_id = max((int(r["league_id"]) for r in rows), default=0)
    matched, added = [], []

    for league in sorted({(l or "").strip() for l in odds_leagues}):
        if not league or league in existing_odds:
            continue

        candidates = [r for r in rows if r["league_espn"].strip() and not r["league_odds"].strip()]
        match_name = fuzzy_best(league, [r["league_espn"].strip() for r in candidates], LEAGUE_MATCH_THRESHOLD)

        if match_name:
            row = next(r for r in candidates if r["league_espn"].strip() == match_name)
            row["league_odds"] = league
            matched.append((league, match_name))
        else:
            max_id += 1
            rows.append({"league_id": str(max_id), "league_espn": "", "league_odds": league, "country": ""})
            added.append(league)

        existing_odds.add(league)

    return matched, added


def reconcile_teams(rows, odds_entries, league_espn_for_odds):
    existing_pairs = {
        (r["league_name"].strip(), r["odds_team_name"].strip())
        for r in rows if r["odds_team_name"].strip()
    }
    max_id = max((int(r["team_id"]) for r in rows), default=0)
    matched, added, unmapped_leagues = [], [], set()

    team_entries = set()
    for e in odds_entries:
        team_entries.add((e["league"], e["home"]))
        team_entries.add((e["league"], e["away"]))

    for league_odds_val, team in sorted((v or "", t or "") for v, t in team_entries):
        league_odds_val, team = league_odds_val.strip(), team.strip()
        if not team:
            continue

        league_espn = league_espn_for_odds.get(league_odds_val)
        if not league_espn:
            if league_odds_val:
                unmapped_leagues.add(league_odds_val)
            continue  # league has no canonical name yet; can't place the team

        if (league_espn, team) in existing_pairs:
            continue

        candidates = [
            r for r in rows
            if r["league_name"].strip() == league_espn
            and r["team_name"].strip() and not r["odds_team_name"].strip()
        ]
        match_name = fuzzy_best(team, [r["team_name"].strip() for r in candidates], TEAM_MATCH_THRESHOLD)

        if match_name:
            row = next(r for r in candidates if r["team_name"].strip() == match_name)
            row["odds_team_name"] = team
            matched.append((league_espn, team, match_name))
        else:
            max_id += 1
            rows.append({"team_id": str(max_id), "team_name": "", "league_name": league_espn, "odds_team_name": team})
            added.append((league_espn, team))

        existing_pairs.add((league_espn, team))

    return matched, added, unmapped_leagues


def main():
    league_fields, league_rows = read_csv(LEAGUES_PATH)
    team_fields, team_rows = read_csv(TEAMS_PATH)

    odds_entries = fetch_odds_entries()
    league_matched, league_added = reconcile_leagues(league_rows, [e["league"] for e in odds_entries])

    league_espn_for_odds = {
        r["league_odds"].strip(): r["league_espn"].strip()
        for r in league_rows if r["league_odds"].strip()
    }
    team_matched, team_added, unmapped_leagues = reconcile_teams(team_rows, odds_entries, league_espn_for_odds)

    write_csv(LEAGUES_PATH, league_fields, league_rows)
    write_csv(TEAMS_PATH, team_fields, team_rows)

    print(f"Leagues: {len(league_matched)} matched, {len(league_added)} added pending review")
    for league, espn in league_matched:
        print(f"  matched: {league!r} -> {espn!r}")
    for league in league_added:
        print(f"  added (needs league_espn + country): {league!r}")

    print(f"Teams: {len(team_matched)} matched, {len(team_added)} added pending review")
    for league_espn, team in team_added:
        print(f"  added (needs team_name): [{league_espn}] {team!r}")

    if unmapped_leagues:
        print(f"Skipped team matching for leagues still pending league_espn: {sorted(unmapped_leagues)}")


if __name__ == "__main__":
    main()

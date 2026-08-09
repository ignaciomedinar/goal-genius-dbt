"""
Reconciles seeds/leagues.csv and seeds/team_names.csv against the league and
team names seen in ESPN scrape results (raw.raw_results) and in the odds
feed (raw.raw_weekly_odds).

Two separate gaps this closes:
  - New leagues seen only in ESPN scrapes get auto-assigned a league_id by
    dim_leagues.sql directly, but with country_id left permanently NULL --
    nothing ever adds them to leagues.csv, so there's no pending-review
    trail. This script appends a row with league_espn filled (it's already
    the canonical name) and country left blank for manual completion.
  - fact_bookmakers_odds joins odds rows to dim_leagues/dim_teams through
    these two seeds. Any odds-side league/team name with no row here gets
    silently dropped from that join. This script closes that gap by filling
    in the odds-side column (league_odds / odds_team_name) on an existing
    seed row when a confident fuzzy match is found.

team_name is a required identity column -- every row must have one, a team
just may have several rows (one per league/cup it plays in). So an odds team
with no match *within its own league* is not immediately given a blank
placeholder row: it's first checked against every team_name already known
for a league in the same country (e.g. a team newly promoted to a
competition it wasn't seeded for yet, like a country making its first
appearance at a continental tournament after previously only appearing in
that tournament's qualifiers). Only when no equivalent exists anywhere is
the odds team left out of the seed entirely and reported as unresolved --
never inserted with a blank team_name.

league_espn is the equivalent required identity column on the leagues side,
and is always populated the same way (dim_leagues.sql/reconcile_espn_leagues
guarantee every ESPN-seen league gets a row). Only league_odds and country
are allowed to be blank pending manual completion.

Any row left with a blank optional field, plus any team that couldn't be
placed at all, is written to PENDING_REPORT_PATH so CI can turn it into an
alert (e.g. a GitHub issue).
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
PENDING_REPORT_PATH = os.path.join(os.path.dirname(__file__), "..", "pending_mappings.md")

# Leagues are matched against ALL other leagues (cross-league name collisions
# are easy, e.g. "FIFA World Cup" vs "FIFA Club World Cup"), so this needs to
# be strict. Teams are matched within a single already-resolved league, where
# look-alikes are rare, so it can be looser to catch things like
# "Athletic Bilbao" vs "Athletic Club".
LEAGUE_MATCH_THRESHOLD = 0.90
TEAM_MATCH_THRESHOLD = 0.72

# Fallback used only when a team has no match within its own league: search
# every team_name already known for a league in the same country. That's a
# much bigger, less-curated pool where unrelated clubs sharing a generic name
# (e.g. "Independiente", "Nacional") are common, so this stays strict and
# prefers an exact match over a fuzzy one.
CROSS_LEAGUE_MATCH_THRESHOLD = 0.90


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


def fetch_espn_leagues():
    with pg_engine.connect() as conn:
        return [r[0] for r in conn.execute(text("select distinct league from raw.raw_results")) if r[0]]


def reconcile_espn_leagues(rows, espn_leagues):
    """Add a pending row for any ESPN league dim_leagues.sql would otherwise
    silently auto-create with no country_id and no seed trail at all."""
    existing_espn = {r["league_espn"].strip() for r in rows if r["league_espn"].strip()}
    max_id = max((int(r["league_id"]) for r in rows), default=0)
    added = []

    for league in sorted({(l or "").strip() for l in espn_leagues}):
        if not league or league in existing_espn:
            continue

        max_id += 1
        rows.append({"league_id": str(max_id), "league_espn": league, "league_odds": "", "country": ""})
        added.append(league)
        existing_espn.add(league)

    return added


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


def _leagues_by_country(league_rows):
    country_for_espn = {}
    espn_leagues_for_country = {}
    for r in league_rows:
        league_espn, country = r["league_espn"].strip(), r["country"].strip()
        if not league_espn or not country:
            continue
        country_for_espn[league_espn] = country
        espn_leagues_for_country.setdefault(country, set()).add(league_espn)
    return country_for_espn, espn_leagues_for_country


def reconcile_teams(rows, odds_entries, league_espn_for_odds, league_rows):
    existing_pairs = {
        (r["league_name"].strip(), r["odds_team_name"].strip())
        for r in rows if r["odds_team_name"].strip()
    }
    max_id = max((int(r["team_id"]) for r in rows), default=0)
    country_for_espn, espn_leagues_for_country = _leagues_by_country(league_rows)
    matched, cross_league, unresolved, unmapped_leagues = [], [], [], set()

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

        # 1) same-league fuzzy match -- the common case, e.g. a spelling
        # variant of a team already known for this exact competition.
        same_league_candidates = [
            r for r in rows
            if r["league_name"].strip() == league_espn
            and r["team_name"].strip() and not r["odds_team_name"].strip()
        ]
        match_name = fuzzy_best(team, [r["team_name"].strip() for r in same_league_candidates], TEAM_MATCH_THRESHOLD)

        if match_name:
            row = next(r for r in same_league_candidates if r["team_name"].strip() == match_name)
            row["odds_team_name"] = team
            matched.append((league_espn, team, match_name))
            existing_pairs.add((league_espn, team))
            continue

        # 2) no match in this league -- the team may still be a known ESPN
        # team just not yet seeded for this particular competition (e.g. a
        # country appearing at a tournament it only had qualifiers for
        # before). Look for it anywhere within the same country, exact name
        # match first, and fall back to a strict fuzzy match.
        country = country_for_espn.get(league_espn)
        sibling_leagues = espn_leagues_for_country.get(country, set()) if country else set()
        known_names = {
            r["team_name"].strip() for r in rows
            if r["team_name"].strip() and r["league_name"].strip() in sibling_leagues
        }

        resolved_name = None
        exact = {n.lower(): n for n in known_names}
        if team.lower() in exact:
            resolved_name = exact[team.lower()]
        elif known_names:
            resolved_name = fuzzy_best(team, known_names, CROSS_LEAGUE_MATCH_THRESHOLD)

        if resolved_name:
            max_id += 1
            rows.append({"team_id": str(max_id), "team_name": resolved_name, "league_name": league_espn, "odds_team_name": team})
            cross_league.append((league_espn, team, resolved_name))
            existing_pairs.add((league_espn, team))
            continue

        # 3) genuinely never seen under any name -- don't fabricate an
        # identity-less row, just flag it for manual research.
        unresolved.append((league_espn, team))

    return matched, cross_league, unresolved, unmapped_leagues


def write_pending_report(league_rows, team_rows, unresolved_teams, unmapped_leagues):
    """Write a summary of everything still needing a human, so CI can turn it
    into an alert. No file is written when nothing is pending, so callers can
    treat "file exists" as "there's something to review".

    team_name/league_espn are identity columns and are never left blank by
    this script (see reconcile_teams/reconcile_espn_leagues), so unresolved
    teams never make it into team_rows at all -- they're passed in directly
    instead of being scanned for out of the seed."""
    pending_leagues_country = sorted(
        r["league_espn"].strip() for r in league_rows
        if r["league_espn"].strip() and not r["country"].strip()
    )
    pending_leagues_espn = sorted(
        r["league_odds"].strip() for r in league_rows
        if r["league_odds"].strip() and not r["league_espn"].strip()
    )
    pending_unresolved_teams = sorted(f"[{league}] {team}" for league, team in unresolved_teams)

    if not (pending_leagues_country or pending_leagues_espn or pending_unresolved_teams):
        if os.path.exists(PENDING_REPORT_PATH):
            os.remove(PENDING_REPORT_PATH)
        return False

    lines = ["# Pending league/team mappings", "", "Auto-generated by `update_seed_mappings.py`. Fill in the blanks in "
             "`goal_genius_db/seeds/leagues.csv` / `team_names.csv` and this list will shrink on the next run.", ""]

    if pending_leagues_country:
        lines.append(f"## Leagues missing `country` ({len(pending_leagues_country)})")
        lines += [f"- {name}" for name in pending_leagues_country]
        lines.append("")

    if pending_leagues_espn:
        lines.append(f"## Odds leagues missing `league_espn` match ({len(pending_leagues_espn)})")
        lines += [f"- {name}" for name in pending_leagues_espn]
        lines.append("")

    if pending_unresolved_teams:
        lines.append(f"## Odds teams with no ESPN equivalent found -- not added to the seed ({len(pending_unresolved_teams)})")
        lines += [f"- {name}" for name in pending_unresolved_teams]
        lines.append("")

    if unmapped_leagues:
        lines.append(f"## Odds teams skipped (league not yet mapped) ({len(unmapped_leagues)})")
        lines += [f"- {name}" for name in sorted(unmapped_leagues)]
        lines.append("")

    with open(PENDING_REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return True


def main():
    league_fields, league_rows = read_csv(LEAGUES_PATH)
    team_fields, team_rows = read_csv(TEAMS_PATH)

    # Purge any identity-less rows left over from before this script enforced
    # team_name as required (or from manual edits). Dropping them here -- as
    # opposed to just never creating new ones -- makes the invariant
    # self-healing: whatever they were meant to represent gets a fresh shot
    # at the same-league/cross-league matching below on every run, instead
    # of staying silently stuck as a blank row forever.
    legacy_blank = [r for r in team_rows if not r["team_name"].strip()]
    if legacy_blank:
        team_rows = [r for r in team_rows if r["team_name"].strip()]
        print(f"Purged {len(legacy_blank)} legacy rows with a blank team_name for re-matching")

    espn_leagues = fetch_espn_leagues()
    espn_league_added = reconcile_espn_leagues(league_rows, espn_leagues)

    odds_entries = fetch_odds_entries()
    league_matched, odds_league_added = reconcile_leagues(league_rows, [e["league"] for e in odds_entries])

    league_espn_for_odds = {
        r["league_odds"].strip(): r["league_espn"].strip()
        for r in league_rows if r["league_odds"].strip()
    }
    team_matched, team_cross_league, team_unresolved, unmapped_leagues = reconcile_teams(
        team_rows, odds_entries, league_espn_for_odds, league_rows
    )

    write_csv(LEAGUES_PATH, league_fields, league_rows)
    write_csv(TEAMS_PATH, team_fields, team_rows)

    print(f"ESPN leagues: {len(espn_league_added)} added pending review")
    for league in espn_league_added:
        print(f"  added (needs country): {league!r}")

    print(f"Odds leagues: {len(league_matched)} matched, {len(odds_league_added)} added pending review")
    for league, espn in league_matched:
        print(f"  matched: {league!r} -> {espn!r}")
    for league in odds_league_added:
        print(f"  added (needs league_espn + country): {league!r}")

    print(f"Teams: {len(team_matched)} matched same-league, {len(team_cross_league)} matched cross-league, "
          f"{len(team_unresolved)} unresolved (not added)")
    for league_espn, team, matched_to in team_cross_league:
        print(f"  cross-league match: [{league_espn}] {team!r} -> {matched_to!r}")
    for league_espn, team in team_unresolved:
        print(f"  unresolved (needs manual research): [{league_espn}] {team!r}")

    if unmapped_leagues:
        print(f"Skipped team matching for leagues still pending league_espn: {sorted(unmapped_leagues)}")

    has_pending = write_pending_report(league_rows, team_rows, team_unresolved, unmapped_leagues)
    print(f"Pending report {'written to ' + PENDING_REPORT_PATH if has_pending else 'skipped (nothing pending)'}")


if __name__ == "__main__":
    main()

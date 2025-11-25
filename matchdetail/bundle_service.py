# ==============================================================
# bundle_service.py (A방식 + comp/last_n 완전 지원)
# ==============================================================

from database.db import db
from services.insights.insights_block import build_insights_overall_block


def fetch_fixture_header(fixture_id: int):
    q = """
        SELECT
            f.fixture_id,
            f.date,
            f.status,
            f.league_id,
            f.season,
            h.team_id AS home_id,
            h.name     AS home_name,
            h.logo     AS home_logo,
            a.team_id AS away_id,
            a.name     AS away_name,
            a.logo     AS away_logo
        FROM fixtures f
        JOIN teams h ON h.team_id = f.home_id
        JOIN teams a ON a.team_id = f.away_id
        WHERE f.fixture_id = %s
    """
    return db.fetch_one(q, (fixture_id,))


def fetch_timeline_block(fixture_id: int):
    q = """
        SELECT *
        FROM match_events
        WHERE fixture_id = %s
        ORDER BY minute ASC, id ASC
    """
    return db.fetch_all(q, (fixture_id,))


def fetch_stats_block(fixture_id: int):
    q = """
        SELECT *
        FROM match_team_stats
        WHERE fixture_id = %s
    """
    rows = db.fetch_all(q, (fixture_id,))
    result = {"home": {}, "away": {}}
    for r in rows:
        side = "home" if r.get("is_home") else "away"
        result[side] = r
    return result


def fetch_lineups_block(fixture_id: int):
    q = """
        SELECT *
        FROM match_lineups
        WHERE fixture_id = %s
    """
    return db.fetch_all(q, (fixture_id,))


def fetch_h2h_block(home_id: int, away_id: int):
    q = """
        SELECT *
        FROM h2h_results
        WHERE (home_id = %s AND away_id = %s)
           OR (home_id = %s AND away_id = %s)
        ORDER BY date DESC
    """
    return db.fetch_all(q, (home_id, away_id, away_id, home_id))


def fetch_standings_block(league_id: int, season_int: int):
    q = """
        SELECT *
        FROM standings
        WHERE league_id = %s AND season = %s
        ORDER BY position ASC
    """
    return db.fetch_all(q, (league_id, season_int))


def build_match_detail_bundle(
    fixture_id: int, league_id: int, season_int: int,
    comp: str, last_n: str
):
    header = fetch_fixture_header(fixture_id)
    if not header:
        return {}

    home_id = header["home_id"]
    away_id = header["away_id"]

    timeline = fetch_timeline_block(fixture_id)
    stats = fetch_stats_block(fixture_id)
    lineups = fetch_lineups_block(fixture_id)
    h2h = fetch_h2h_block(home_id, away_id)
    standings = fetch_standings_block(league_id, season_int)

    insights_overall = build_insights_overall_block(
        league_id=league_id,
        season_int=season_int,
        home_team_id=home_id,
        away_team_id=away_id,
        comp=comp,
        last_n_raw=last_n
    )

    return {
        "header": header,
        "timeline": timeline,
        "stats": stats,
        "lineups": lineups,
        "h2h": {
            "rows": h2h,
            "summary": {}
        },
        "standings": {
            "rows": standings
        },
        "insights_overall": insights_overall
    }

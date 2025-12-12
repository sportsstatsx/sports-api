from __future__ import annotations

import os
import time
import argparse
import logging
from typing import Any, Dict, List, Optional

import requests
import atexit
import db as dbmod
from db import execute, fetch_all

log = logging.getLogger("hockey_backfill")
logging.basicConfig(level=logging.INFO)

# =========================================================
# DB pool 정리
# =========================================================
def _close_db_pool():
    try:
        if hasattr(dbmod, "pool") and dbmod.pool:
            dbmod.pool.close()
    except Exception:
        pass

atexit.register(_close_db_pool)

# =========================================================
# API Client
# =========================================================
class HockeyApi:
    BASE_URL = "https://v1.hockey.api-sports.io"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = requests.get(
            f"{self.BASE_URL}{path}",
            headers={"x-apisports-key": self.api_key},
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def leagues(self):
        return self._get("/leagues", {})

    def games(self, league_id: int, season: int):
        return self._get("/games", {"league": league_id, "season": season})

    def game_events(self, game_id: int):
        try:
            return self._get("/games/events", {"game": game_id})
        except requests.HTTPError:
            return self._get("/games", {"game": game_id})

    def standings(self, league_id: int, season: int):
        return self._get("/standings", {"league": league_id, "season": season})

# =========================================================
# Utils
# =========================================================
def safe_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def safe_text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None

# =========================================================
# Upserts
# =========================================================
def upsert_country(c: Dict[str, Any]):
    execute(
        """
        INSERT INTO hockey_countries (id, name, code, flag)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          name=EXCLUDED.name,
          code=EXCLUDED.code,
          flag=EXCLUDED.flag
        """,
        (safe_int(c.get("id")), safe_text(c.get("name")), safe_text(c.get("code")), safe_text(c.get("flag"))),
    )

def upsert_league(l: Dict[str, Any], country: Dict[str, Any]):
    if country:
        upsert_country(country)

    execute(
        """
        INSERT INTO hockey_leagues (id, name, type, logo, country_id)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          name=EXCLUDED.name,
          type=EXCLUDED.type,
          logo=EXCLUDED.logo,
          country_id=EXCLUDED.country_id
        """,
        (
            safe_int(l.get("id")),
            safe_text(l.get("name")),
            safe_text(l.get("type")),
            safe_text(l.get("logo")),
            safe_int(country.get("id")) if country else None,
        ),
    )

def upsert_league_season(league_id: int, s: Dict[str, Any]):
    execute(
        """
        INSERT INTO hockey_league_seasons
          (league_id, season, current, start_date, end_date)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (league_id, season) DO UPDATE SET
          current=EXCLUDED.current,
          start_date=EXCLUDED.start_date,
          end_date=EXCLUDED.end_date
        """,
        (
            league_id,
            safe_int(s.get("season")),
            bool(s.get("current")),
            s.get("start"),
            s.get("end"),
        ),
    )

def upsert_team(t: Dict[str, Any]):
    country = t.get("country")
    if isinstance(country, dict):
        upsert_country(country)
        country_id = safe_int(country.get("id"))
    else:
        country_id = None

    execute(
        """
        INSERT INTO hockey_teams (id, name, logo, country_id)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          name=EXCLUDED.name,
          logo=EXCLUDED.logo,
          country_id=EXCLUDED.country_id
        """,
        (
            safe_int(t.get("id")),
            safe_text(t.get("name")),
            safe_text(t.get("logo")),
            country_id,
        ),
    )

def upsert_game(game: Dict[str, Any], league_id: int, season: int) -> Optional[int]:
    g = game.get("game") or game
    teams = game.get("teams") or {}

    home = teams.get("home") or {}
    away = teams.get("away") or {}

    if home:
        upsert_team(home)
    if away:
        upsert_team(away)

    game_id = safe_int(g.get("id"))
    if not game_id:
        return None

    status = g.get("status") or {}
    status_short = status.get("short") if isinstance(status, dict) else safe_text(status)
    status_long = status.get("long") if isinstance(status, dict) else None

    execute(
        """
        INSERT INTO hockey_games (
          id, league_id, season,
          stage, group_name,
          home_team_id, away_team_id,
          game_date, status, status_long, timezone,
          score_json, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT (id) DO UPDATE SET
          status=EXCLUDED.status,
          status_long=EXCLUDED.status_long,
          score_json=EXCLUDED.score_json,
          raw_json=EXCLUDED.raw_json
        """,
        (
            game_id,
            league_id,
            season,
            safe_text(game.get("league", {}).get("stage")),
            safe_text(game.get("league", {}).get("group")),
            safe_int(home.get("id")),
            safe_int(away.get("id")),
            g.get("date"),
            status_short,
            status_long,
            g.get("timezone"),
            game.get("scores") or {},
            game,
        ),
    )
    return game_id

def insert_game_event(game_id: int, ev: Dict[str, Any], order: int):
    execute(
        """
        INSERT INTO hockey_game_events (
          game_id, period, minute, team_id,
          type, comment, players, assists,
          event_order, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (game_id, period, minute, team_id, type, event_order)
        DO UPDATE SET
          comment=EXCLUDED.comment,
          players=EXCLUDED.players,
          assists=EXCLUDED.assists,
          raw_json=EXCLUDED.raw_json
        WHERE
          hockey_game_events.raw_json IS DISTINCT FROM EXCLUDED.raw_json
        """,
        (
            game_id,
            safe_text(ev.get("period")) or "UNK",
            safe_int(ev.get("minute")),
            safe_int((ev.get("team") or {}).get("id")),
            safe_text(ev.get("type")) or "unknown",
            safe_text(ev.get("comment")),
            ev.get("players") or [],
            ev.get("assists") or [],
            order,
            ev,
        ),
    )

def upsert_standings(league_id: int, season: int, payload: Dict[str, Any]):
    resp = payload.get("response") or []
    for block in resp:
        if not isinstance(block, list):
            continue
        for row in block:
            team = row.get("team") or {}
            upsert_team(team)

            execute(
                """
                INSERT INTO hockey_standings (
                  league_id, season, stage, group_name,
                  team_id, position,
                  games_played, win_total, win_pct,
                  lose_total, lose_pct,
                  goals_for, goals_against,
                  points, form, description, raw_json
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT (league_id, season, stage, group_name, team_id)
                DO UPDATE SET
                  position=EXCLUDED.position,
                  points=EXCLUDED.points,
                  raw_json=EXCLUDED.raw_json
                """,
                (
                    league_id,
                    season,
                    safe_text(row.get("stage")),
                    safe_text((row.get("group") or {}).get("name")),
                    safe_int(team.get("id")),
                    safe_int(row.get("position")),
                    safe_int(row.get("games", {}).get("played")),
                    safe_int(row.get("win", {}).get("total")),
                    safe_text(row.get("win", {}).get("percentage")),
                    safe_int(row.get("lose", {}).get("total")),
                    safe_text(row.get("lose", {}).get("percentage")),
                    safe_int(row.get("goals", {}).get("for")),
                    safe_int(row.get("goals", {}).get("against")),
                    safe_int(row.get("points")),
                    safe_text(row.get("form")),
                    safe_text(row.get("description")),
                    row,
                ),
            )

# =========================================================
# main
# =========================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True)
    ap.add_argument("--league-id", required=True)
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--skip-events", action="store_true")
    ap.add_argument("--skip-future-events", action="store_true")
    args = ap.parse_args()

    api_key = (
        os.environ.get("APISPORTS_KEY")
        or os.environ.get("API_SPORTS_KEY")
        or os.environ.get("APIFOOTBALL_KEY")
    )
    if not api_key:
        raise SystemExit("APISPORTS_KEY not set")

    api = HockeyApi(api_key)
    season = int(args.season)
    league_ids = [int(x) for x in args.league_id.split(",")]

    # leagues + seasons
    leagues_payload = api.leagues()
    for item in leagues_payload.get("response") or []:
        league = item.get("league")
        country = item.get("country")
        seasons = item.get("seasons") or []
        upsert_league(league, country)
        for s in seasons:
            upsert_league_season(safe_int(league.get("id")), s)

    for lid in league_ids:
        games_payload = api.games(lid, season)
        game_ids: List[int] = []

        for g in games_payload.get("response") or []:
            gid = upsert_game(g, lid, season)
            if gid:
                game_ids.append(gid)

        if not args.skip_events:
            for gid in game_ids:
                meta = fetch_all(
                    "SELECT status, game_date FROM hockey_games WHERE id=%s",
                    (gid,),
                )[0]
                if args.skip_future_events and (
                    meta["status"] in ("NS", "TBD") or meta["game_date"] is None
                ):
                    continue

                ev_payload = api.game_events(gid)
                for idx, ev in enumerate(ev_payload.get("response") or []):
                    insert_game_event(gid, ev, idx)
                time.sleep(args.sleep)

        st = api.standings(lid, season)
        upsert_standings(lid, season, st)

    log.info("✅ hockey backfill complete")

if __name__ == "__main__":
    main()

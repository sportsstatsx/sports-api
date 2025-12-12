from __future__ import annotations

import os
import time
import json
import argparse
import logging
from typing import Any, Dict, List, Optional

import requests

from db import execute, fetch_all, fetch_one

log = logging.getLogger("hockey_backfill")
logging.basicConfig(level=logging.INFO)


# ──────────────────────────────────────────────────────────────
# API Client (API-Sports Hockey)
# ──────────────────────────────────────────────────────────────
class HockeyApi:
    BASE_URL = "https://v1.hockey.api-sports.io"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = requests.get(
            f"{self.BASE_URL}{path}",
            headers={"x-apisports-key": self.api_key},
            params=params,
            timeout=40,
        )
        r.raise_for_status()
        return r.json()

    def league_by_id(self, league_id: int) -> Dict[str, Any]:
        # /leagues 는 응답 형태가 들쭉날쭉할 수 있어서, 필요한 리그만 id로 조회
        return self._get("/leagues", {"id": league_id})

    def games(self, league_id: int, season: int) -> Dict[str, Any]:
        return self._get("/games", {"league": league_id, "season": season})

    def game_events(self, game_id: int) -> Dict[str, Any]:
        # 공식 엔드포인트
        return self._get("/games/events", {"game": game_id})

    def standings(self, league_id: int, season: int) -> Dict[str, Any]:
        return self._get("/standings", {"league": league_id, "season": season})


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def safe_int(v) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def safe_text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def safe_num(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────
# Upserts (schema: 001_init_hockey.sql 기준)
# ──────────────────────────────────────────────────────────────
def upsert_country(country: Dict[str, Any]):
    if not isinstance(country, dict):
        return
    cid = safe_int(country.get("id"))
    name = safe_text(country.get("name"))
    if cid is None or not name:
        return

    execute(
        """
        INSERT INTO hockey_countries (id, name, code, flag)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          code = EXCLUDED.code,
          flag = EXCLUDED.flag
        """,
        (cid, name, safe_text(country.get("code")), safe_text(country.get("flag"))),
    )


def upsert_league(league: Dict[str, Any], country: Optional[Dict[str, Any]]):
    if not isinstance(league, dict):
        return
    lid = safe_int(league.get("id"))
    name = safe_text(league.get("name"))
    ltype = safe_text(league.get("type"))
    if lid is None or not name or not ltype:
        return

    if isinstance(country, dict):
        upsert_country(country)
        country_id = safe_int(country.get("id"))
    else:
        country_id = None

    execute(
        """
        INSERT INTO hockey_leagues (id, name, type, logo, country_id)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          type = EXCLUDED.type,
          logo = EXCLUDED.logo,
          country_id = EXCLUDED.country_id
        """,
        (lid, name, ltype, safe_text(league.get("logo")), country_id),
    )


def upsert_league_season(league_id: int, season_obj: Dict[str, Any]):
    if not isinstance(season_obj, dict):
        return
    season = safe_int(season_obj.get("season"))
    if season is None:
        return

    execute(
        """
        INSERT INTO hockey_league_seasons
          (league_id, season, current, start_date, end_date)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (league_id, season) DO UPDATE SET
          current = EXCLUDED.current,
          start_date = EXCLUDED.start_date,
          end_date = EXCLUDED.end_date
        """,
        (
            league_id,
            season,
            bool(season_obj.get("current")),
            season_obj.get("start"),
            season_obj.get("end"),
        ),
    )


def upsert_team(team: Dict[str, Any]):
    if not isinstance(team, dict):
        return
    tid = safe_int(team.get("id"))
    name = safe_text(team.get("name"))
    if tid is None or not name:
        return

    country = team.get("country")
    country_id = None
    if isinstance(country, dict):
        upsert_country(country)
        country_id = safe_int(country.get("id"))

    execute(
        """
        INSERT INTO hockey_teams (id, name, logo, country_id)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          logo = EXCLUDED.logo,
          country_id = EXCLUDED.country_id
        """,
        (tid, name, safe_text(team.get("logo")), country_id),
    )


def upsert_game(game_item: Dict[str, Any], league_id: int, season: int) -> Optional[int]:
    if not isinstance(game_item, dict):
        return None

    g = game_item.get("game") or {}
    if not isinstance(g, dict):
        return None

    gid = safe_int(g.get("id"))
    if gid is None:
        return None

    league_obj = game_item.get("league") or {}
    teams_obj = game_item.get("teams") or {}
    scores_obj = game_item.get("scores") or {}

    # teams upsert
    home = (teams_obj.get("home") or {}) if isinstance(teams_obj, dict) else {}
    away = (teams_obj.get("away") or {}) if isinstance(teams_obj, dict) else {}
    if isinstance(home, dict) and home.get("id"):
        upsert_team(home)
    if isinstance(away, dict) and away.get("id"):
        upsert_team(away)

    status_obj = g.get("status") or {}
    status = safe_text(status_obj.get("short")) if isinstance(status_obj, dict) else safe_text(status_obj)
    status_long = safe_text(status_obj.get("long")) if isinstance(status_obj, dict) else None

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
          league_id = EXCLUDED.league_id,
          season = EXCLUDED.season,
          stage = EXCLUDED.stage,
          group_name = EXCLUDED.group_name,
          home_team_id = EXCLUDED.home_team_id,
          away_team_id = EXCLUDED.away_team_id,
          game_date = EXCLUDED.game_date,
          status = EXCLUDED.status,
          status_long = EXCLUDED.status_long,
          timezone = EXCLUDED.timezone,
          score_json = EXCLUDED.score_json,
          raw_json = EXCLUDED.raw_json
        """,
        (
            gid,
            league_id,
            season,
            safe_text(league_obj.get("stage")) if isinstance(league_obj, dict) else None,
            safe_text(league_obj.get("group")) if isinstance(league_obj, dict) else None,
            safe_int(home.get("id")) if isinstance(home, dict) else None,
            safe_int(away.get("id")) if isinstance(away, dict) else None,
            g.get("date"),
            status,
            status_long,
            safe_text(g.get("timezone")),
            json.dumps(scores_obj or {}),
            json.dumps(game_item or {}),
        ),
    )

    return gid


def insert_game_event_if_absent(game_id: int, ev: Dict[str, Any], order: int):
    if not isinstance(ev, dict):
        return

    period = safe_text(ev.get("period")) or "UNK"
    minute = safe_int(ev.get("minute"))
    team = ev.get("team") or {}
    team_id = safe_int(team.get("id")) if isinstance(team, dict) else None
    etype = safe_text(ev.get("type")) or "unknown"
    comment = safe_text(ev.get("comment"))

    players = ev.get("players") or []
    assists = ev.get("assists") or []
    # TEXT[]로 들어가야 하므로, 문자열만 남김
    if not isinstance(players, list):
        players = []
    if not isinstance(assists, list):
        assists = []
    players = [safe_text(x) for x in players if safe_text(x)]
    assists = [safe_text(x) for x in assists if safe_text(x)]

    # 스키마에 유니크 제약이 없어서 ON CONFLICT 업서트 불가.
    # 대신 "같은 game_id + period + minute + team_id + type + event_order" 조합이 없을 때만 insert.
    execute(
        """
        INSERT INTO hockey_game_events (
          game_id, period, minute, team_id,
          type, comment, players, assists,
          event_order, raw_json
        )
        SELECT
          %s, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s::jsonb
        WHERE NOT EXISTS (
          SELECT 1
          FROM hockey_game_events
          WHERE game_id = %s
            AND period = %s
            AND minute IS NOT DISTINCT FROM %s
            AND team_id IS NOT DISTINCT FROM %s
            AND type = %s
            AND event_order = %s
        )
        """,
        (
            game_id, period, minute, team_id,
            etype, comment, players, assists,
            order, json.dumps(ev),

            game_id, period, minute, team_id, etype, order
        ),
    )


def upsert_standings(league_id: int, season: int, payload: Dict[str, Any]):
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list):
        return

    # API-Sports standings는 보통 "list of lists" 구조
    for block in resp:
        if not isinstance(block, list):
            continue
        for row in block:
            if not isinstance(row, dict):
                continue

            team = row.get("team") or {}
            if isinstance(team, dict) and team.get("id"):
                upsert_team(team)

            group = row.get("group") or {}
            win = row.get("win") or {}
            win_ot = row.get("win_overtime") or {}
            lose = row.get("lose") or {}
            lose_ot = row.get("lose_overtime") or {}
            goals = row.get("goals") or {}

            execute(
                """
                INSERT INTO hockey_standings (
                  league_id, season, stage, group_name,
                  team_id, position,
                  games_played,
                  win_total, win_pct, win_ot_total, win_ot_pct,
                  lose_total, lose_pct, lose_ot_total, lose_ot_pct,
                  goals_for, goals_against,
                  points,
                  form, description,
                  raw_json
                )
                VALUES (
                  %s,%s,%s,%s,
                  %s,%s,
                  %s,
                  %s,%s,%s,%s,
                  %s,%s,%s,%s,
                  %s,%s,
                  %s,
                  %s,%s,
                  %s::jsonb
                )
                ON CONFLICT (league_id, season, stage, group_name, team_id)
                DO UPDATE SET
                  position = EXCLUDED.position,
                  games_played = EXCLUDED.games_played,
                  win_total = EXCLUDED.win_total,
                  win_pct = EXCLUDED.win_pct,
                  win_ot_total = EXCLUDED.win_ot_total,
                  win_ot_pct = EXCLUDED.win_ot_pct,
                  lose_total = EXCLUDED.lose_total,
                  lose_pct = EXCLUDED.lose_pct,
                  lose_ot_total = EXCLUDED.lose_ot_total,
                  lose_ot_pct = EXCLUDED.lose_ot_pct,
                  goals_for = EXCLUDED.goals_for,
                  goals_against = EXCLUDED.goals_against,
                  points = EXCLUDED.points,
                  form = EXCLUDED.form,
                  description = EXCLUDED.description,
                  raw_json = EXCLUDED.raw_json
                """,
                (
                    league_id,
                    season,
                    safe_text(row.get("stage")),
                    safe_text(group.get("name")) if isinstance(group, dict) else None,

                    safe_int(team.get("id")) if isinstance(team, dict) else None,
                    safe_int(row.get("position")),

                    safe_int((row.get("games") or {}).get("played")) if isinstance(row.get("games"), dict) else None,

                    safe_int(win.get("total")) if isinstance(win, dict) else None,
                    safe_num(win.get("percentage")) if isinstance(win, dict) else None,
                    safe_int(win_ot.get("total")) if isinstance(win_ot, dict) else None,
                    safe_num(win_ot.get("percentage")) if isinstance(win_ot, dict) else None,

                    safe_int(lose.get("total")) if isinstance(lose, dict) else None,
                    safe_num(lose.get("percentage")) if isinstance(lose, dict) else None,
                    safe_int(lose_ot.get("total")) if isinstance(lose_ot, dict) else None,
                    safe_num(lose_ot.get("percentage")) if isinstance(lose_ot, dict) else None,

                    safe_int(goals.get("for")) if isinstance(goals, dict) else None,
                    safe_int(goals.get("against")) if isinstance(goals, dict) else None,

                    safe_int(row.get("points")),

                    safe_text(row.get("form")),
                    safe_text(row.get("description")),

                    json.dumps(row),
                ),
            )


# ──────────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--league-id", required=True)  # comma-separated
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--skip-events", action="store_true")
    ap.add_argument("--skip-future-events", action="store_true")
    args = ap.parse_args()

    api_key = os.environ.get("APISPORTS_KEY") or os.environ.get("API_SPORTS_KEY")
    if not api_key:
        raise SystemExit("APISPORTS_KEY (or API_SPORTS_KEY) is not set")

    api = HockeyApi(api_key)
    season = args.season
    league_ids = [int(x.strip()) for x in args.league_id.split(",") if x.strip()]

    # 리그/시즌 메타는 필요한 리그만 id로 조회해서 안정적으로 upsert
    for lid in league_ids:
        meta = api.league_by_id(lid)
        items = meta.get("response") if isinstance(meta, dict) else None
        if not isinstance(items, list) or not items:
            log.warning("league meta empty for league_id=%s", lid)
            continue

        # 보통 첫 원소에 league/country/seasons가 들어있음
        item0 = items[0] if isinstance(items[0], dict) else {}
        league_obj = item0.get("league")
        country_obj = item0.get("country")
        seasons_obj = item0.get("seasons") or []

        if isinstance(league_obj, dict):
            upsert_league(league_obj, country_obj if isinstance(country_obj, dict) else None)
            for s in seasons_obj:
                upsert_league_season(lid, s if isinstance(s, dict) else {})
        else:
            log.warning("league object missing for league_id=%s", lid)

    # games -> events -> standings
    for lid in league_ids:
        games_payload = api.games(lid, season)
        resp = games_payload.get("response") if isinstance(games_payload, dict) else None
        if not isinstance(resp, list):
            log.warning("games response invalid for league_id=%s season=%s", lid, season)
            continue

        game_ids: List[int] = []
        for item in resp:
            gid = upsert_game(item, lid, season)
            if gid:
                game_ids.append(gid)

        log.info("league=%s season=%s games_upserted=%s", lid, season, len(game_ids))

        if not args.skip_events:
            for gid in game_ids:
                # 미래/NS 스킵 옵션
                if args.skip_future_events:
                    row = fetch_one("SELECT status, game_date FROM hockey_games WHERE id=%s", (gid,))
                    if row:
                        st = row.get("status")
                        gd = row.get("game_date")
                        # status NS/TBD면 이벤트 호출 불필요
                        if st in ("NS", "TBD"):
                            continue
                        # 날짜가 미래면 스킵
                        if gd is not None:
                            chk = fetch_one("SELECT (%s::timestamptz > NOW()) AS is_future", (gd,))
                            if chk and chk.get("is_future"):
                                continue

                ev_payload = api.game_events(gid)
                ev_list = ev_payload.get("response") if isinstance(ev_payload, dict) else None
                if isinstance(ev_list, list) and ev_list:
                    for idx, ev in enumerate(ev_list):
                        insert_game_event_if_absent(gid, ev if isinstance(ev, dict) else {}, idx)

                time.sleep(args.sleep)

        st_payload = api.standings(lid, season)
        upsert_standings(lid, season, st_payload)
        log.info("league=%s season=%s standings_upserted", lid, season)

    log.info("✅ hockey backfill complete")


if __name__ == "__main__":
    main()

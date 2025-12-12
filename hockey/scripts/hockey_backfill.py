from __future__ import annotations

import os
import time
import json
import argparse
import logging
from typing import Any, Dict, List, Optional

import requests

from db import execute, fetch_one

log = logging.getLogger("hockey_backfill")
logging.basicConfig(level=logging.INFO)


class HockeyApi:
    BASE_URL = "https://v1.hockey.api-sports.io"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = requests.get(
            f"{self.BASE_URL}{path}",
            headers={"x-apisports-key": self.api_key},
            params=params,
            timeout=45,
        )
        r.raise_for_status()
        return r.json()

    def league_by_id(self, league_id: int) -> Dict[str, Any]:
        return self._get("/leagues", {"id": league_id})

    def games(self, league_id: int, season: int) -> Dict[str, Any]:
        return self._get("/games", {"league": league_id, "season": season})

    def game_events(self, game_id: int) -> Dict[str, Any]:
        return self._get("/games/events", {"game": game_id})

    def standings(self, league_id: int, season: int) -> Dict[str, Any]:
        return self._get("/standings", {"league": league_id, "season": season})


def safe_int(v) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def safe_text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def safe_float(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def upsert_country(country: Dict[str, Any]) -> None:
    if not isinstance(country, dict):
        return
    cid = safe_int(country.get("id"))
    name = safe_text(country.get("name"))
    if cid is None or not name:
        return

    execute(
        """
        INSERT INTO hockey_countries (id, name, code, flag, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          code = EXCLUDED.code,
          flag = EXCLUDED.flag,
          updated_at = NOW()
        """,
        (cid, name, safe_text(country.get("code")), safe_text(country.get("flag"))),
    )


def upsert_league(league: Dict[str, Any], country: Optional[Dict[str, Any]]) -> None:
    if not isinstance(league, dict):
        return
    lid = safe_int(league.get("id"))
    name = safe_text(league.get("name"))
    ltype = safe_text(league.get("type"))
    if lid is None or not name or not ltype:
        return

    country_id = None
    if isinstance(country, dict):
        upsert_country(country)
        country_id = safe_int(country.get("id"))

    execute(
        """
        INSERT INTO hockey_leagues (id, name, type, logo, country_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          type = EXCLUDED.type,
          logo = EXCLUDED.logo,
          country_id = EXCLUDED.country_id,
          updated_at = NOW()
        """,
        (lid, name, ltype, safe_text(league.get("logo")), country_id),
    )


def upsert_league_season(league_id: int, s: Dict[str, Any]) -> None:
    if not isinstance(s, dict):
        return
    season = safe_int(s.get("season"))
    if season is None:
        return

    execute(
        """
        INSERT INTO hockey_league_seasons (league_id, season, current, start_date, end_date, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (league_id, season) DO UPDATE SET
          current = EXCLUDED.current,
          start_date = EXCLUDED.start_date,
          end_date = EXCLUDED.end_date,
          updated_at = NOW()
        """,
        (league_id, season, bool(s.get("current", False)), s.get("start"), s.get("end")),
    )


def upsert_team(team: Dict[str, Any]) -> None:
    if not isinstance(team, dict):
        return
    tid = safe_int(team.get("id"))
    name = safe_text(team.get("name"))
    if tid is None or tid == 0 or not name:
        return

    country_id = None
    c = team.get("country")
    if isinstance(c, dict):
        upsert_country(c)
        country_id = safe_int(c.get("id"))

    execute(
        """
        INSERT INTO hockey_teams (id, name, logo, country_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          logo = EXCLUDED.logo,
          country_id = EXCLUDED.country_id,
          updated_at = NOW()
        """,
        (tid, name, safe_text(team.get("logo")), country_id),
    )


def upsert_game(item: Dict[str, Any], league_id: int, season: int) -> Optional[int]:
    if not isinstance(item, dict):
        return None

    g = item.get("game")
    if not isinstance(g, dict):
        return None

    gid = safe_int(g.get("id"))
    if gid is None:
        return None

    league_obj = item.get("league") if isinstance(item.get("league"), dict) else {}
    teams_obj = item.get("teams") if isinstance(item.get("teams"), dict) else {}
    scores_obj = item.get("scores") if isinstance(item.get("scores"), dict) else {}

    home = teams_obj.get("home") if isinstance(teams_obj.get("home"), dict) else {}
    away = teams_obj.get("away") if isinstance(teams_obj.get("away"), dict) else {}

    if home.get("id"):
        upsert_team(home)
    if away.get("id"):
        upsert_team(away)

    status_obj = g.get("status") if isinstance(g.get("status"), dict) else {}
    status = safe_text(status_obj.get("short"))
    status_long = safe_text(status_obj.get("long"))

    execute(
        """
        INSERT INTO hockey_games (
          id, league_id, season,
          stage, group_name,
          home_team_id, away_team_id,
          game_date, status, status_long, timezone,
          score_json, raw_json,
          created_at, updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,NOW(),NOW())
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
          raw_json = EXCLUDED.raw_json,
          updated_at = NOW()
        """,
        (
            gid,
            league_id,
            season,
            safe_text(league_obj.get("stage")),
            safe_text(league_obj.get("group")),
            safe_int(home.get("id")),
            safe_int(away.get("id")),
            g.get("date"),
            status,
            status_long,
            safe_text(g.get("timezone")),
            json.dumps(scores_obj),
            json.dumps(item),
        ),
    )
    return gid


def upsert_event(game_id: int, ev: Dict[str, Any], order: int) -> None:
    if not isinstance(ev, dict):
        return

    period = safe_text(ev.get("period")) or "UNK"
    minute = safe_int(ev.get("minute"))
    team = ev.get("team") if isinstance(ev.get("team"), dict) else {}
    team_id = safe_int(team.get("id")) if isinstance(team, dict) else None
    etype = safe_text(ev.get("type")) or "unknown"
    comment = safe_text(ev.get("comment"))

    players = ev.get("players")
    assists = ev.get("assists")
    if not isinstance(players, list):
        players = []
    if not isinstance(assists, list):
        assists = []
    players = [safe_text(x) for x in players if safe_text(x)]
    assists = [safe_text(x) for x in assists if safe_text(x)]

    execute(
        """
        INSERT INTO hockey_game_events (
          game_id, period, minute, team_id,
          type, comment, players, assists,
          event_order, raw_json,
          created_at, updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW(),NOW())
        ON CONFLICT (game_id, period, minute, team_id, type, event_order)
        DO UPDATE SET
          comment = EXCLUDED.comment,
          players = EXCLUDED.players,
          assists = EXCLUDED.assists,
          raw_json = EXCLUDED.raw_json,
          updated_at = NOW()
        """,
        (
            game_id, period, minute, team_id,
            etype, comment, players, assists,
            order, json.dumps(ev),
        ),
    )


def _iter_standings_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    API-Sports hockey standings는 보통:
    response = [ { "league": {...}, "standings": [ [row,row..], [row..] ] }, ... ]
    형태가 많음. (리그 1개 요청해도 리스트로 올 수 있음)
    """
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list):
        return []

    out: List[Dict[str, Any]] = []

    for entry in resp:
        if isinstance(entry, dict):
            st = entry.get("standings")
            if isinstance(st, list):
                for block in st:
                    if isinstance(block, list):
                        for row in block:
                            if isinstance(row, dict):
                                out.append(row)
                    elif isinstance(block, dict):
                        out.append(block)
            continue

        # 혹시 response가 바로 [ [row...] ] 형태로 올 때 대비
        if isinstance(entry, list):
            for row in entry:
                if isinstance(row, dict):
                    out.append(row)

    return out


def upsert_standings(league_id: int, season: int, payload: Dict[str, Any]) -> int:
    rows = _iter_standings_rows(payload)
    if not rows:
        return 0

    saved = 0
    for row in rows:
        team = row.get("team") if isinstance(row.get("team"), dict) else {}
        team_id = safe_int(team.get("id")) if isinstance(team, dict) else None
        if not team_id or team_id == 0:
            continue  # ✅ NOT NULL / FK 보호

        upsert_team(team)

        # stage/group_name은 NOT NULL이므로 기본값 강제
        stage = safe_text(row.get("stage")) or "REGULAR"

        group_name: Optional[str] = None
        g = row.get("group")
        if isinstance(g, dict):
            group_name = safe_text(g.get("name"))
        elif isinstance(g, str):
            group_name = safe_text(g)
        group_name = group_name or "ALL"

        win = row.get("win") if isinstance(row.get("win"), dict) else {}
        win_ot = row.get("win_overtime") if isinstance(row.get("win_overtime"), dict) else {}
        lose = row.get("lose") if isinstance(row.get("lose"), dict) else {}
        lose_ot = row.get("lose_overtime") if isinstance(row.get("lose_overtime"), dict) else {}
        goals = row.get("goals") if isinstance(row.get("goals"), dict) else {}
        games = row.get("games") if isinstance(row.get("games"), dict) else {}

        execute(
            """
            INSERT INTO hockey_standings (
              league_id, season, stage, group_name,
              team_id, position,
              games_played,
              win_total, win_pct, win_ot_total, win_ot_pct,
              lose_total, lose_pct, lose_ot_total, lose_ot_pct,
              goals_for, goals_against,
              points, form, description, raw_json,
              created_at, updated_at
            )
            VALUES (
              %s,%s,%s,%s,
              %s,%s,
              %s,
              %s,%s,%s,%s,
              %s,%s,%s,%s,
              %s,%s,
              %s,%s,%s,%s::jsonb,
              NOW(),NOW()
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
              raw_json = EXCLUDED.raw_json,
              updated_at = NOW()
            """,
            (
                league_id,
                season,
                stage,
                group_name,
                team_id,
                safe_int(row.get("position")) or 0,

                safe_int(games.get("played")) if isinstance(games, dict) else None,

                safe_int(win.get("total")) if isinstance(win, dict) else None,
                safe_float(win.get("percentage")) if isinstance(win, dict) else None,
                safe_int(win_ot.get("total")) if isinstance(win_ot, dict) else None,
                safe_float(win_ot.get("percentage")) if isinstance(win_ot, dict) else None,

                safe_int(lose.get("total")) if isinstance(lose, dict) else None,
                safe_float(lose.get("percentage")) if isinstance(lose, dict) else None,
                safe_int(lose_ot.get("total")) if isinstance(lose_ot, dict) else None,
                safe_float(lose_ot.get("percentage")) if isinstance(lose_ot, dict) else None,

                safe_int(goals.get("for")) if isinstance(goals, dict) else None,
                safe_int(goals.get("against")) if isinstance(goals, dict) else None,

                safe_int(row.get("points")),
                safe_text(row.get("form")),
                safe_text(row.get("description")),
                json.dumps(row),
            ),
        )
        saved += 1

    return saved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--league-id", required=True)  # comma-separated
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--skip-future-events", action="store_true")
    args = ap.parse_args()

    api_key = os.environ.get("APISPORTS_KEY") or os.environ.get("API_SPORTS_KEY")
    if not api_key:
        raise SystemExit("APISPORTS_KEY (or API_SPORTS_KEY) is not set")

    api = HockeyApi(api_key)
    season = args.season
    league_ids = [int(x.strip()) for x in args.league_id.split(",") if x.strip()]

    # 1) league meta upsert
    for lid in league_ids:
        meta = api.league_by_id(lid)
        resp = meta.get("response") if isinstance(meta, dict) else None
        if not isinstance(resp, list) or not resp or not isinstance(resp[0], dict):
            raise SystemExit(f"league meta invalid: league_id={lid} meta={meta}")

        item = resp[0]

        # API 형태: item["league"], item["country"], item["seasons"]
        league_obj = item.get("league") if isinstance(item.get("league"), dict) else item
        country_obj = item.get("country") if isinstance(item.get("country"), dict) else None
        seasons_obj = item.get("seasons") if isinstance(item.get("seasons"), list) else []

        upsert_league(league_obj, country_obj)
        for s in seasons_obj:
            if isinstance(s, dict):
                upsert_league_season(lid, s)

        # FK 방지: 리그가 DB에 실제로 들어갔는지 확인
        chk = fetch_one("SELECT id FROM hockey_leagues WHERE id=%s", (lid,))
        if not chk:
            raise SystemExit(f"league upsert failed: league_id={lid}")

    # 2) games + teams
    for lid in league_ids:
        games_payload = api.games(lid, season)
        resp = games_payload.get("response") if isinstance(games_payload, dict) else None
        if not isinstance(resp, list):
            raise SystemExit(f"games response invalid: league_id={lid} season={season} payload={games_payload}")

        game_ids: List[int] = []
        for item in resp:
            gid = upsert_game(item if isinstance(item, dict) else {}, lid, season)
            if gid:
                game_ids.append(gid)

        log.info("league=%s season=%s games_upserted=%s", lid, season, len(game_ids))

        # 3) events
        for gid in game_ids:
            if args.skip_future_events:
                row = fetch_one("SELECT status, game_date FROM hockey_games WHERE id=%s", (gid,))
                if row:
                    st = row.get("status")
                    gd = row.get("game_date")
                    if st in ("NS", "TBD"):
                        continue
                    if gd is not None:
                        chk = fetch_one("SELECT (%s::timestamptz > NOW()) AS is_future", (gd,))
                        if chk and chk.get("is_future"):
                            continue

            ev_payload = api.game_events(gid)
            ev_list = ev_payload.get("response") if isinstance(ev_payload, dict) else None
            if isinstance(ev_list, list):
                for idx, ev in enumerate(ev_list):
                    if isinstance(ev, dict):
                        upsert_event(gid, ev, idx)

            time.sleep(args.sleep)

        # 4) standings (마지막)
        st_payload = api.standings(lid, season)
        saved = upsert_standings(lid, season, st_payload)
        log.info("league=%s season=%s standings_saved=%s", lid, season, saved)

    log.info("✅ hockey backfill complete")


if __name__ == "__main__":
    main()

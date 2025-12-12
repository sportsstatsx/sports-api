from __future__ import annotations

import os
import time
import json
import argparse
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import execute, fetch_one
from db import pool as db_pool  # db.py 에 pool 변수가 있어야 함

log = logging.getLogger("hockey_backfill")
logging.basicConfig(level=logging.INFO)


# ─────────────────────────────────────────
# API Client (API-Sports Hockey)
# ─────────────────────────────────────────
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


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────
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


def jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


# ─────────────────────────────────────────
# Upserts (001_init_hockey.sql 기준)
# ─────────────────────────────────────────
def upsert_country(country: Dict[str, Any]) -> None:
    if not isinstance(country, dict):
        return
    cid = safe_int(country.get("id"))
    name = safe_text(country.get("name"))
    if cid is None or not name:
        return

    execute(
        """
        INSERT INTO hockey_countries (id, name, code, flag)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          code = EXCLUDED.code,
          flag = EXCLUDED.flag
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
        INSERT INTO hockey_leagues (id, name, type, logo, country_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          type = EXCLUDED.type,
          logo = EXCLUDED.logo,
          country_id = EXCLUDED.country_id
        """,
        (lid, name, ltype, safe_text(league.get("logo")), country_id),
    )


def upsert_league_season(league_id: int, season: int, current: bool = False,
                        start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
    execute(
        """
        INSERT INTO hockey_league_seasons (league_id, season, current, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (league_id, season) DO UPDATE SET
          current = EXCLUDED.current,
          start_date = EXCLUDED.start_date,
          end_date = EXCLUDED.end_date
        """,
        (league_id, season, bool(current), start_date, end_date),
    )


def upsert_team(team: Dict[str, Any]) -> None:
    if not isinstance(team, dict):
        return
    tid = safe_int(team.get("id"))
    name = safe_text(team.get("name"))
    if tid is None or tid == 0 or not name:
        return

    # games/standings의 team 객체에는 country가 없을 수도 있음(정상)
    country_id = None
    c = team.get("country")
    if isinstance(c, dict):
        upsert_country(c)
        country_id = safe_int(c.get("id"))

    execute(
        """
        INSERT INTO hockey_teams (id, name, logo, country_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          logo = EXCLUDED.logo,
          country_id = EXCLUDED.country_id
        """,
        (tid, name, safe_text(team.get("logo")), country_id),
    )


def upsert_game_from_games_endpoint(item: Dict[str, Any], league_id_arg: int, season_arg: int) -> Optional[int]:
    """
    ✅ API-Sports hockey /games 응답은 "id/date/status/league/teams/scores/periods/events"가 최상위.
    """
    if not isinstance(item, dict):
        return None

    gid = safe_int(item.get("id"))
    if gid is None:
        return None

    # league info (fallback)
    league_obj = item.get("league") if isinstance(item.get("league"), dict) else {}
    league_id = safe_int(league_obj.get("id")) or league_id_arg
    season = safe_int(league_obj.get("season")) or season_arg

    # ensure league exists (혹시 meta 단계에서 못 넣었어도 games로 복구)
    country_obj = item.get("country") if isinstance(item.get("country"), dict) else None
    if isinstance(country_obj, dict):
        upsert_country(country_obj)
    if isinstance(league_obj, dict) and safe_int(league_obj.get("id")):
        upsert_league(league_obj, country_obj)
        # 시즌도 최소 1개는 넣어둠(정확 start/end는 leagues meta에서만 나옴)
        upsert_league_season(league_id, season, current=False)

    teams_obj = item.get("teams") if isinstance(item.get("teams"), dict) else {}
    home = teams_obj.get("home") if isinstance(teams_obj.get("home"), dict) else {}
    away = teams_obj.get("away") if isinstance(teams_obj.get("away"), dict) else {}

    if home.get("id"):
        upsert_team(home)
    if away.get("id"):
        upsert_team(away)

    status_obj = item.get("status") if isinstance(item.get("status"), dict) else {}
    status = safe_text(status_obj.get("short"))
    status_long = safe_text(status_obj.get("long"))

    score_obj = item.get("scores")
    if not isinstance(score_obj, (dict, list)):
        score_obj = {}

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
            safe_text(league_obj.get("stage")),
            safe_text(league_obj.get("group")),
            safe_int(home.get("id")),
            safe_int(away.get("id")),
            item.get("date"),
            status,
            status_long,
            safe_text(item.get("timezone")),
            jdump(score_obj),
            jdump(item),
        ),
    )
    return gid


def upsert_event(game_id: int, ev: Dict[str, Any], order: int) -> None:
    """
    events 중복방지:
      - 동일 game_id 안에서 같은 period/minute/team/type/order 조합은 같은 이벤트로 보고 upsert
      - team_id가 0/None이면 FK 때문에 스킵(또는 team 없는 이벤트로 저장하려면 schema/정책 변경 필요)
    """
    if not isinstance(ev, dict):
        return

    period = safe_text(ev.get("period")) or "UNK"
    minute = safe_int(ev.get("minute"))

    team = ev.get("team") if isinstance(ev.get("team"), dict) else {}
    team_id = safe_int(team.get("id")) if isinstance(team, dict) else None
    if team_id == 0:
        team_id = None

    # team이 있으면 팀 upsert 선행
    if isinstance(team, dict) and team.get("id"):
        upsert_team(team)

    etype = safe_text(ev.get("type")) or "unknown"
    comment = safe_text(ev.get("comment")) or safe_text(ev.get("detail"))

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
          event_order, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (game_id, period, minute, team_id, type, event_order)
        DO UPDATE SET
          comment = EXCLUDED.comment,
          players = EXCLUDED.players,
          assists = EXCLUDED.assists,
          raw_json = EXCLUDED.raw_json
        WHERE hockey_game_events.raw_json IS DISTINCT FROM EXCLUDED.raw_json
        """,
        (
            game_id,
            period,
            minute,
            team_id,
            etype,
            comment,
            players,
            assists,
            order,
            jdump(ev),
        ),
    )


def _normalize_standings_blocks(payload: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
    """
    standings 응답 형태가 케이스가 여러개라 통일:
    - case A: response = [ { league: { standings: [[...], [...]] } } ]
    - case B: response = [[...],[...]]
    """
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list) or not resp:
        return []

    # A
    if isinstance(resp[0], dict):
        league = resp[0].get("league")
        if isinstance(league, dict):
            st = league.get("standings")
            if isinstance(st, list):
                # st가 [[{...}]] 형태
                if st and isinstance(st[0], list):
                    return st  # type: ignore
                # st가 [{...}] 형태면 1블록으로 래핑
                if st and isinstance(st[0], dict):
                    return [st]  # type: ignore

    # B
    if isinstance(resp[0], list):
        return resp  # type: ignore

    return []


def upsert_standings(league_id: int, season: int, payload: Dict[str, Any]) -> int:
    blocks = _normalize_standings_blocks(payload)
    if not blocks:
        return 0

    saved = 0
    for block in blocks:
        if not isinstance(block, list):
            continue

        for row in block:
            if not isinstance(row, dict):
                continue

            team = row.get("team") if isinstance(row.get("team"), dict) else {}
            team_id = safe_int(team.get("id")) if isinstance(team, dict) else None
            if team_id is None or team_id == 0:
                continue  # ✅ FK 방지(0/None 팀은 버림)

            upsert_team(team)

            group = row.get("group") if isinstance(row.get("group"), dict) else {}
            win = row.get("win") if isinstance(row.get("win"), dict) else {}
            win_ot = row.get("win_overtime") if isinstance(row.get("win_overtime"), dict) else {}
            lose = row.get("lose") if isinstance(row.get("lose"), dict) else {}
            lose_ot = row.get("lose_overtime") if isinstance(row.get("lose_overtime"), dict) else {}
            goals = row.get("goals") if isinstance(row.get("goals"), dict) else {}
            games = row.get("games") if isinstance(row.get("games"), dict) else {}

            # ✅ schema에서 stage/group_name NOT NULL 이라 기본값 보정
            stage = safe_text(row.get("stage")) or "REG"
            group_name = safe_text(group.get("name")) or "overall"

            execute(
                """
                INSERT INTO hockey_standings (
                  league_id, season, stage, group_name,
                  team_id, position,
                  games_played,
                  win_total, win_pct, win_ot_total, win_ot_pct,
                  lose_total, lose_pct, lose_ot_total, lose_ot_pct,
                  goals_for, goals_against,
                  points, form, description, raw_json
                )
                VALUES (
                  %s,%s,%s,%s,
                  %s,%s,
                  %s,
                  %s,%s,%s,%s,
                  %s,%s,%s,%s,
                  %s,%s,
                  %s,%s,%s,%s::jsonb
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
                    stage,
                    group_name,
                    team_id,
                    safe_int(row.get("position")) or 0,
                    safe_int(games.get("played")),

                    safe_int(win.get("total")),
                    safe_float(win.get("percentage")),
                    safe_int(win_ot.get("total")),
                    safe_float(win_ot.get("percentage")),

                    safe_int(lose.get("total")),
                    safe_float(lose.get("percentage")),
                    safe_int(lose_ot.get("total")),
                    safe_float(lose_ot.get("percentage")),

                    safe_int(goals.get("for")),
                    safe_int(goals.get("against")),

                    safe_int(row.get("points")),
                    safe_text(row.get("form")),
                    safe_text(row.get("description")),
                    jdump(row),
                ),
            )
            saved += 1

    return saved


# ─────────────────────────────────────────
# main
# ─────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--league-id", required=True)  # comma-separated
    ap.add_argument("--sleep", type=float, default=0.1)

    ap.add_argument("--events-source", choices=["auto", "inline", "endpoint", "skip"], default="auto")
    ap.add_argument("--skip-future-events", action="store_true")
    ap.add_argument("--with-standings", action="store_true", default=False)
    args = ap.parse_args()

    api_key = os.environ.get("APISPORTS_KEY") or os.environ.get("API_SPORTS_KEY")
    if not api_key:
        raise SystemExit("APISPORTS_KEY (or API_SPORTS_KEY) is not set")

    api = HockeyApi(api_key)
    season = args.season
    league_ids = [int(x.strip()) for x in args.league_id.split(",") if x.strip()]

    try:
        # 1) league meta upsert
        for lid in league_ids:
            meta = api.league_by_id(lid)
            resp = meta.get("response") if isinstance(meta, dict) else None
            if not isinstance(resp, list) or not resp or not isinstance(resp[0], dict):
                log.warning("league meta empty/invalid: league_id=%s meta=%s", lid, meta)
                continue

            item = resp[0]
            league_obj = item.get("league") if isinstance(item.get("league"), dict) else item
            country_obj = item.get("country") if isinstance(item.get("country"), dict) else None
            seasons_obj = item.get("seasons") if isinstance(item.get("seasons"), list) else []

            if not isinstance(league_obj, dict) or safe_int(league_obj.get("id")) is None:
                log.warning("league object missing: league_id=%s item=%s", lid, item)
                continue

            upsert_league(league_obj, country_obj)

            # leagues meta에 있는 시즌들 저장
            for s in seasons_obj:
                if not isinstance(s, dict):
                    continue
                ss = safe_int(s.get("season"))
                if ss is None:
                    continue
                upsert_league_season(
                    lid,
                    ss,
                    current=bool(s.get("current", False)),
                    start_date=s.get("start"),
                    end_date=s.get("end"),
                )

            # sanity check
            chk = fetch_one("SELECT id FROM hockey_leagues WHERE id=%s", (lid,))
            if not chk:
                log.error("❌ league upsert failed: league_id=%s", lid)
                raise SystemExit(2)

        # 2) games -> events
        for lid in league_ids:
            games_payload = api.games(lid, season)
            resp = games_payload.get("response") if isinstance(games_payload, dict) else None
            if not isinstance(resp, list):
                log.warning("games response invalid: league_id=%s season=%s payload=%s", lid, season, games_payload)
                continue

            game_ids: List[int] = []
            for item in resp:
                gid = upsert_game_from_games_endpoint(item if isinstance(item, dict) else {}, lid, season)
                if gid:
                    game_ids.append(gid)

            log.info("league=%s season=%s games_upserted=%s", lid, season, len(game_ids))

            # events
            if args.events_source != "skip":
                for gid in game_ids:
                    if args.skip_future_events:
                        row = fetch_one("SELECT status, game_date FROM hockey_games WHERE id=%s", (gid,))
                        if row:
                            st = row.get("status")
                            gd = row.get("game_date")
                            if st in ("NS", "TBD"):
                                continue
                            if gd is not None:
                                chk2 = fetch_one("SELECT (%s::timestamptz > NOW()) AS is_future", (gd,))
                                if chk2 and chk2.get("is_future"):
                                    continue

                    ev_list: Optional[List[Any]] = None

                    if args.events_source in ("auto", "inline"):
                        # games raw_json 안의 events를 우선 사용
                        r = fetch_one("SELECT raw_json FROM hockey_games WHERE id=%s", (gid,))
                        if r and isinstance(r.get("raw_json"), dict):
                            inline = r["raw_json"].get("events")
                            if isinstance(inline, list):
                                ev_list = inline

                    if ev_list is None and args.events_source in ("auto", "endpoint"):
                        ev_payload = api.game_events(gid)
                        resp2 = ev_payload.get("response") if isinstance(ev_payload, dict) else None
                        if isinstance(resp2, list):
                            ev_list = resp2

                    if isinstance(ev_list, list):
                        for idx, ev in enumerate(ev_list):
                            if isinstance(ev, dict):
                                upsert_event(gid, ev, idx)

                    time.sleep(args.sleep)

            # 3) standings
            if args.with_standings:
                st_payload = api.standings(lid, season)
                saved = upsert_standings(lid, season, st_payload)
                log.info("league=%s season=%s standings_saved=%s", lid, season, saved)

        log.info("✅ hockey backfill complete")

    finally:
        # psycopg pool warning 제거(프로세스 종료 시 thread 정리)
        try:
            db_pool.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

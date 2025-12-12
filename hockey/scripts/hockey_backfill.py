from __future__ import annotations

import os
import time
import argparse
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests
import atexit
import db as dbmod

from db import execute, fetch_all  # 너 프로젝트의 db 헬퍼

log = logging.getLogger("hockey_backfill")
logging.basicConfig(level=logging.INFO)


# =========================
# 프로세스 종료 시 psycopg pool 정리
# =========================
def _close_db_pool():
    try:
        if hasattr(dbmod, "pool") and dbmod.pool:
            dbmod.pool.close()
    except Exception:
        pass


atexit.register(_close_db_pool)


# =========================
# API-Sports Hockey Client
# =========================
class HockeyApi:
    def __init__(self, api_key: str, base_url: str = "https://v1.hockey.api-sports.io"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"x-apisports-key": self.api_key}
        r = requests.get(url, headers=headers, params=params or {}, timeout=30)
        r.raise_for_status()
        return r.json()

    def leagues(self) -> Dict[str, Any]:
        return self.get("/leagues")

    def games(self, league_id: int, season: int) -> Dict[str, Any]:
        # Hockey /games 는 page 파라미터를 지원하지 않음
        return self.get("/games", {"league": league_id, "season": season})

    def standings(self, league_id: int, season: int) -> Dict[str, Any]:
        return self.get("/standings", {"league": league_id, "season": season})

    def game_events(self, game_id: int) -> Dict[str, Any]:
        # 일부 문서/예제에서 /games?game=... 또는 /games/events?game=...
        # 둘 다 시도해보고 성공하는 쪽을 사용.
        try:
            return self.get("/games/events", {"game": game_id})
        except requests.HTTPError:
            return self.get("/games", {"game": game_id})

    def odds(self, game_id: int) -> Dict[str, Any]:
        return self.get("/odds", {"id": game_id})


# =========================
# utils
# =========================
def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(str(x))
    except Exception:
        return None


def safe_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def fetch_one(sql: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


# =========================
# DB UPSERTS (스키마 기준)
# =========================
def upsert_country(country: Dict[str, Any]) -> None:
    sql = """
    INSERT INTO hockey_countries (id, name, code, flag)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      code = EXCLUDED.code,
      flag = EXCLUDED.flag;
    """
    execute(
        sql,
        (
            safe_int(country.get("id")),
            safe_text(country.get("name")),
            safe_text(country.get("code")),
            safe_text(country.get("flag")),
        ),
    )


def upsert_league(league: Dict[str, Any], country: Optional[Dict[str, Any]]) -> None:
    if country and isinstance(country, dict):
        upsert_country(country)

    sql = """
    INSERT INTO hockey_leagues (id, name, type, logo, country_id, country_name, season, raw_json)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      type = EXCLUDED.type,
      logo = EXCLUDED.logo,
      country_id = EXCLUDED.country_id,
      country_name = EXCLUDED.country_name,
      season = EXCLUDED.season,
      raw_json = EXCLUDED.raw_json;
    """
    execute(
        sql,
        (
            safe_int(league.get("id")),
            safe_text(league.get("name")),
            safe_text(league.get("type")),
            safe_text(league.get("logo")),
            safe_int(country.get("id")) if country else None,
            safe_text(country.get("name")) if country else None,
            safe_int(league.get("season")) or None,
            str(league).replace("'", '"'),
        ),
    )


def upsert_team(team: Dict[str, Any]) -> None:
    sql = """
    INSERT INTO hockey_teams (id, name, code, logo, country, founded, national, raw_json)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      code = EXCLUDED.code,
      logo = EXCLUDED.logo,
      country = EXCLUDED.country,
      founded = EXCLUDED.founded,
      national = EXCLUDED.national,
      raw_json = EXCLUDED.raw_json;
    """
    execute(
        sql,
        (
            safe_int(team.get("id")),
            safe_text(team.get("name")),
            safe_text(team.get("code")),
            safe_text(team.get("logo")),
            safe_text(team.get("country")),
            safe_int(team.get("founded")),
            bool(team.get("national")) if team.get("national") is not None else None,
            str(team).replace("'", '"'),
        ),
    )


def insert_game(game: Dict[str, Any], league_id: int, season: int) -> Optional[int]:
    # API 응답 구조가 {game:{}, league:{}, teams:{}, scores:{}} 형태일 수 있음
    # 또는 {id:..., date:...} 등 flat 일 수 있어 방어적으로.
    g = game.get("game") if isinstance(game.get("game"), dict) else game
    league = game.get("league") if isinstance(game.get("league"), dict) else {}
    teams = game.get("teams") if isinstance(game.get("teams"), dict) else {}
    scores = game.get("scores") if isinstance(game.get("scores"), dict) else game.get("score", {})

    game_id = safe_int(g.get("id") or g.get("game") or game.get("id"))
    if not game_id:
        return None

    # 팀
    home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
    away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
    if home:
        upsert_team(home)
    if away:
        upsert_team(away)

    # 상태/일정
    status = None
    status_long = None
    st = g.get("status")
    if isinstance(st, dict):
        status = safe_text(st.get("short") or st.get("status") or st.get("code"))
        status_long = safe_text(st.get("long") or st.get("description"))
    else:
        status = safe_text(st)

    game_date = g.get("date") or g.get("start") or g.get("time") or g.get("timestamp")
    timezone = safe_text(g.get("timezone") or game.get("timezone"))

    # score_json / raw_json
    score_json = scores if isinstance(scores, dict) else {}
    raw_json = game

    sql = """
    INSERT INTO hockey_games (
      id, league_id, season, stage, group_name,
      home_team_id, away_team_id,
      game_date, status, status_long, timezone,
      score_json, raw_json
    )
    VALUES (
      %s, %s, %s, %s, %s,
      %s, %s,
      %s, %s, %s, %s,
      %s::jsonb, %s::jsonb
    )
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
      raw_json = EXCLUDED.raw_json;
    """

    execute(
        sql,
        (
            game_id,
            league_id,
            season,
            safe_text(league.get("stage") or league.get("round") or game.get("stage")),
            safe_text(league.get("group") or league.get("group_name") or game.get("group")),
            safe_int(home.get("id")) if home else None,
            safe_int(away.get("id")) if away else None,
            game_date,
            status,
            status_long,
            timezone,
            str(score_json).replace("'", '"'),
            str(raw_json).replace("'", '"'),
        ),
    )

    return game_id


def insert_game_event(game_id: int, ev: Dict[str, Any], event_order: int) -> None:
    period = safe_text(ev.get("period") or ev.get("periods") or ev.get("time") or ev.get("elapsed")) or "UNK"
    minute_i = safe_int(ev.get("minute") or ev.get("time"))
    team = ev.get("team") if isinstance(ev.get("team"), dict) else {}
    team_i = safe_int(team.get("id")) if team else safe_int(ev.get("team_id"))

    # type/comment
    etype = safe_text(ev.get("type") or ev.get("event") or ev.get("name")) or "unknown"
    comment = safe_text(ev.get("comment") or ev.get("detail") or ev.get("description"))

    # players / assists
    players = ev.get("players") or []
    assists = ev.get("assists") or []
    if not isinstance(players, list):
        players = []
    if not isinstance(assists, list):
        assists = []

    sql = """
    INSERT INTO hockey_game_events (
      game_id, period, minute, team_id,
      type, comment,
      players, assists,
      event_order,
      raw_json
    )
    VALUES (
      %s, %s, %s, %s,
      %s, %s,
      %s, %s,
      %s,
      %s::jsonb
    )
    ON CONFLICT (game_id, period, minute, team_id, type, event_order) DO UPDATE SET
      comment = EXCLUDED.comment,
      players = EXCLUDED.players,
      assists = EXCLUDED.assists,
      raw_json = EXCLUDED.raw_json
    WHERE hockey_game_events.comment IS DISTINCT FROM EXCLUDED.comment
       OR hockey_game_events.players IS DISTINCT FROM EXCLUDED.players
       OR hockey_game_events.assists IS DISTINCT FROM EXCLUDED.assists
       OR hockey_game_events.raw_json IS DISTINCT FROM EXCLUDED.raw_json;
    """
    execute(
        sql,
        (
            game_id,
            period,
            minute_i,
            team_i,
            etype,
            comment,
            players,
            assists,
            event_order,
            str(ev).replace("'", '"'),
        ),
    )


# =========================
# Odds meta tables (markets/bookmakers) helper
# =========================
def get_or_create_market_id(name: str) -> int:
    row = fetch_one("SELECT id FROM hockey_odds_markets WHERE name = %s", (name,))
    if row:
        return int(row["id"])
    row2 = fetch_one("INSERT INTO hockey_odds_markets (name) VALUES (%s) RETURNING id", (name,))
    return int(row2["id"])


def get_or_create_bookmaker_id(name: str) -> int:
    row = fetch_one("SELECT id FROM hockey_odds_bookmakers WHERE name = %s", (name,))
    if row:
        return int(row["id"])
    row2 = fetch_one("INSERT INTO hockey_odds_bookmakers (name) VALUES (%s) RETURNING id", (name,))
    return int(row2["id"])


def insert_odds(game_id: int, odds_payload: Dict[str, Any]) -> None:
    # 구조가 리그/플랜에 따라 크게 바뀔 수 있어 방어적으로 저장
    resp = odds_payload.get("response") or []
    if not isinstance(resp, list) or not resp:
        return

    # API-Sports odds는 다양한 형태:
    # response[0].bookmakers[].bets[].values[]
    top = resp[0] if isinstance(resp[0], dict) else None
    if not top:
        return

    bookmakers = top.get("bookmakers") or []
    if not isinstance(bookmakers, list):
        return

    for bm in bookmakers:
        if not isinstance(bm, dict):
            continue
        bm_name = safe_text(bm.get("name")) or "unknown"
        bm_id = get_or_create_bookmaker_id(bm_name)

        bets = bm.get("bets") or []
        if not isinstance(bets, list):
            continue

        for bet in bets:
            if not isinstance(bet, dict):
                continue
            market_name = safe_text(bet.get("name")) or "unknown"
            market_id = get_or_create_market_id(market_name)

            values = bet.get("values") or []
            if not isinstance(values, list):
                continue

            for v in values:
                if not isinstance(v, dict):
                    continue
                outcome = safe_text(v.get("value") or v.get("outcome")) or "unknown"
                odd = safe_text(v.get("odd") or v.get("price"))

                sql = """
                INSERT INTO hockey_odds (
                  game_id, bookmaker_id, market_id, outcome, odd, raw_json
                )
                VALUES (
                  %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (game_id, bookmaker_id, market_id, outcome) DO UPDATE SET
                  odd = EXCLUDED.odd,
                  raw_json = EXCLUDED.raw_json;
                """
                execute(
                    sql,
                    (
                        game_id,
                        bm_id,
                        market_id,
                        outcome,
                        odd,
                        str(v).replace("'", '"'),
                    ),
                )


# =========================
# main
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, help="예: 2025 (2025-2026 시즌 용도)")
    ap.add_argument("--league-id", required=True, help="예: 57 (NHL). 여러 개면 57,58,35")
    ap.add_argument("--sleep", type=float, default=0.25, help="API 호출간 sleep")
    ap.add_argument("--only-pages", type=int, default=0, help="테스트용: games page 몇 페이지만 돌릴지(0이면 전체)")
    ap.add_argument("--skip-events", action="store_true", help="events 스킵(기본은 포함)")
    ap.add_argument("--skip-odds", action="store_true", help="odds 스킵(기본은 포함)")
    ap.add_argument("--only-missing-events", action="store_true", help="이미 이벤트가 있는 game_id는 events 호출/업서트 스킵")
    ap.add_argument("--skip-future-events", action="store_true", help="status=NS/TBD 이거나 game_date가 미래인 경기는 events 스킵 (기본 권장)")
    ap.add_argument("--events-include-ns", action="store_true", help="NS(예정) 경기에도 events 호출 시도(권장X)")
    args = ap.parse_args()

    api_key = (
        os.environ.get("APISPORTS_KEY")
        or os.environ.get("API_SPORTS_KEY")
        or os.environ.get("APIFOOTBALL_KEY")  # Render에 이미 있는 키명 fallback
        or os.environ.get("API_KEY")  # 혹시 공용 키명으로 쓰는 경우 대비
    )
    if not api_key:
        raise SystemExit("APISPORTS_KEY (또는 API_SPORTS_KEY / APIFOOTBALL_KEY / API_KEY) 가 필요합니다.")

    api = HockeyApi(api_key=api_key)

    season = int(args.season)
    league_ids = [int(x.strip()) for x in str(args.league_id).split(",") if x.strip()]

    log.info("✅ hockey_backfill 시작 season=%s league_ids=%s", season, league_ids)

    for lid in league_ids:
        # 1) games
        log.info("1) games upsert 시작 league=%s season=%s", lid, season)
        games_payload = api.games(league_id=lid, season=season)
        resp = games_payload.get("response") or []
        if not isinstance(resp, list):
            resp = []

        game_ids: List[int] = []
        for item in resp:
            if not isinstance(item, dict):
                continue
            gid = insert_game(item, league_id=lid, season=season)
            if gid:
                game_ids.append(gid)

        log.info("   games upsert 완료 league=%s season=%s games=%d", lid, season, len(set(game_ids)))

        # 2) standings
        try:
            log.info("2) standings upsert 시작 league=%s season=%s", lid, season)
            st = api.standings(league_id=lid, season=season)
            # standings는 너의 다른 스크립트에서 처리하거나 필요시 추가 구현 가능
            # 여기서는 호출만(응답 확인 목적) 하고 저장은 생략(필요하면 말해줘)
            _ = st.get("response")
        except requests.HTTPError:
            pass

        # 3) events
        if not args.skip_events:
            log.info("3) game_events upsert 시작 league=%s season=%s games=%d", lid, season, len(set(game_ids)))

            # events 최적화:
            # - already_has_events: DB에 이미 이벤트가 있는 게임은 스킵(옵션)
            # - skip_future_events: status=NS/TBD 또는 game_date가 미래인 게임은 스킵(옵션, 권장)
            game_id_set = sorted(set(game_ids))
            already_has_events: set[int] = set()
            if args.only_missing_events:
                rows = fetch_all(
                    """
                    SELECT DISTINCT e.game_id AS game_id
                    FROM hockey_game_events e
                    JOIN hockey_games g ON g.id = e.game_id
                    WHERE g.league_id = %s AND g.season = %s
                    """,
                    (lid, season),
                )
                already_has_events = {int(r["game_id"]) for r in rows if r.get("game_id") is not None}

            eligible_ids = game_id_set
            if args.skip_future_events and not args.events_include_ns and game_id_set:
                meta_rows = fetch_all(
                    """
                    SELECT id, status, game_date
                    FROM hockey_games
                    WHERE league_id = %s AND season = %s AND id = ANY(%s)
                    """,
                    (lid, season, game_id_set),
                )
                status_by_id = {int(r["id"]): (r.get("status"), r.get("game_date")) for r in meta_rows}
                now_sql = fetch_all("SELECT NOW() AS now", ())[0]["now"]
                filtered: List[int] = []
                for gid in game_id_set:
                    st, gd = status_by_id.get(gid, (None, None))
                    st_norm = (st or "").upper()
                    if st_norm in ("NS", "TBD"):
                        continue
                    if gd is not None and gd > now_sql:
                        continue
                    filtered.append(gid)
                eligible_ids = filtered

            if args.only_missing_events:
                eligible_ids = [gid for gid in eligible_ids if gid not in already_has_events]

            log.info(
                "   events 대상 game_id=%d (전체=%d, already=%d)",
                len(eligible_ids),
                len(game_id_set),
                len(already_has_events),
            )

            for gid in eligible_ids:
                try:
                    ev = api.game_events(gid)
                    ev_resp = ev.get("response") or []
                    if not isinstance(ev_resp, list):
                        time.sleep(args.sleep)
                        continue

                    for idx, item in enumerate(ev_resp):
                        if isinstance(item, dict):
                            insert_game_event(gid, item, idx)
                        elif isinstance(item, list):
                            for j, inner in enumerate(item):
                                if isinstance(inner, dict):
                                    insert_game_event(gid, inner, j)

                    time.sleep(args.sleep)
                except requests.HTTPError:
                    # events 없는 경기 많음 -> 조용히 스킵
                    time.sleep(args.sleep)
                    continue

        # 4) odds
        if not args.skip_odds:
            log.info("4) odds upsert 시작 league=%s season=%s games=%d", lid, season, len(set(game_ids)))
            for gid in sorted(set(game_ids)):
                try:
                    o = api.odds(gid)
                    insert_odds(gid, o)
                    time.sleep(args.sleep)
                except requests.HTTPError:
                    time.sleep(args.sleep)
                    continue

        log.info("✅ league=%s season=%s 백필 완료", lid, season)

    log.info("✅ 전체 백필 완료 (season=%s, league_ids=%s)", season, league_ids)


if __name__ == "__main__":
    main()

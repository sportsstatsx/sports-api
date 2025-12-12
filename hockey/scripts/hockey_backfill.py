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
# [ADD ✅ 정확한 위치] 프로세스 종료 시 psycopg pool 정리 훅
# - 위치: logging 설정 바로 아래 (클래스/함수 정의 시작 전)
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

    # 이벤트/오즈는 리그마다/플랜마다 응답이 없을 수 있어.
    # 있으면 저장하고, 없으면 스킵(에러 무시)하도록 구현.
    def game_events(self, game_id: int) -> Dict[str, Any]:
        # 일부 문서/예제에서 /games?game=... 또는 /games/events?game=...
        # 둘 다 시도해보고 성공하는 쪽을 사용.
        try:
            return self.get("/games/events", {"game": game_id})
        except requests.HTTPError:
            return self.get("/games", {"game": game_id})

    def odds(self, game_id: int) -> Dict[str, Any]:
        # API-Sports hockey odds: 보통 /odds?id=... 형태가 많음
        return self.get("/odds", {"id": game_id})


# =========================
# utils
# =========================
def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj or {}, ensure_ascii=False, separators=(",", ":"))

def num_or_none(x: Any):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None

def fetch_one(sql: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


# =========================
# DB UPSERTS (기존 스키마 기준)
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
    execute(sql, (country.get("id"), country.get("name"), country.get("code"), country.get("flag")))

def upsert_league_flat(item: Dict[str, Any]) -> None:
    # /leagues 의 response item이 flat 구조:
    # {id,name,type,logo,country:{id...}, seasons:[{season,current,start,end},...]}
    c = item.get("country") or {}
    lid = item.get("id")
    country_id = c.get("id")

    if country_id is not None:
        upsert_country(c)

    sql = """
    INSERT INTO hockey_leagues (id, name, type, logo, country_id)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      type = EXCLUDED.type,
      logo = EXCLUDED.logo,
      country_id = EXCLUDED.country_id;
    """
    execute(sql, (lid, item.get("name"), item.get("type"), item.get("logo"), country_id))

def upsert_league_season(league_id: int, s: Dict[str, Any]) -> None:
    sql = """
    INSERT INTO hockey_league_seasons (league_id, season, current, start_date, end_date)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (league_id, season) DO UPDATE SET
      current = EXCLUDED.current,
      start_date = EXCLUDED.start_date,
      end_date = EXCLUDED.end_date;
    """
    execute(sql, (league_id, s.get("season"), bool(s.get("current", False)), s.get("start"), s.get("end")))

def upsert_team(team: Dict[str, Any], country_id: Optional[int]) -> None:
    tid = team.get("id")
    if tid is None:
        log.warning("[team skip] team_id is None team=%s", json_dumps(team))
        return
    try:
        tid_i = int(tid)
    except Exception:
        log.warning("[team skip] invalid team_id=%s team=%s", tid, json_dumps(team))
        return
    if tid_i <= 0:
        log.warning("[team skip] invalid team_id=%s team=%s", tid_i, json_dumps(team))
        return

    sql = """
    INSERT INTO hockey_teams (id, name, logo, country_id)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      logo = EXCLUDED.logo,
      country_id = EXCLUDED.country_id;
    """
    execute(sql, (tid_i, team.get("name"), team.get("logo"), country_id))


def upsert_game(row: Dict[str, Any]) -> Optional[int]:
    """
    /games response row는 보통:
    { game:{id,date,timezone,status{short,long}}, league:{id,season,stage,group,country?}, teams:{home,away}, scores:{...} }
    그런데 리그/플랜에 따라 키가 조금씩 다를 수 있으니 최대한 유연하게 처리.
    """
    game_obj = row.get("game") or row.get("fixture") or row.get("Game") or {}
    league_obj = row.get("league") or row.get("League") or {}
    teams_obj = row.get("teams") or row.get("Teams") or {}
    scores_obj = row.get("scores") or row.get("score") or row.get("Score") or {}

    game_id = game_obj.get("id") or row.get("id")
    if not game_id:
        return None

    league_id = league_obj.get("id")
    season = league_obj.get("season") or row.get("season")

    home = teams_obj.get("home") or {}
    away = teams_obj.get("away") or {}
    home_id = home.get("id")
    away_id = away.get("id")

    game_date = game_obj.get("date") or row.get("date")
    status_obj = game_obj.get("status") or row.get("status") or {}
    if isinstance(status_obj, dict):
        status = status_obj.get("short") or status_obj.get("status")
        status_long = status_obj.get("long") or status_obj.get("description")
    else:
        status = status_obj
        status_long = None

    tz = game_obj.get("timezone") or row.get("timezone")

    stage = league_obj.get("stage")
    group_name = league_obj.get("group")  # 보통 string

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
            int(game_id),
            league_id,
            season,
            stage,
            group_name,
            home_id,
            away_id,
            game_date,
            status,
            status_long,
            tz,
            json_dumps(scores_obj),
            json_dumps(row),
        ),
    )
    return int(game_id)

def upsert_standings(league_id: int, season: int, standings_resp: Dict[str, Any]) -> int:
    response = standings_resp.get("response") or []
    rows: List[Dict[str, Any]] = []

    if len(response) > 0 and isinstance(response[0], list):
        for inner in response:
            rows.extend(inner)
    else:
        rows = response

    count = 0
    for r in rows:
        team = r.get("team") or {}
        group = (r.get("group") or {})
        stage = r.get("stage")
        group_name = group.get("name") if isinstance(group, dict) else r.get("group")

        games = r.get("games") or {}
        win = r.get("win") or {}
        lose = r.get("lose") or {}
        win_ot = r.get("win_overtime") or {}
        lose_ot = r.get("lose_overtime") or {}
        goals = r.get("goals") or {}

        sql = """
        INSERT INTO hockey_standings (
          league_id, season, stage, group_name,
          team_id, position,
          games_played,
          win_total, win_pct, win_ot_total, win_ot_pct,
          lose_total, lose_pct, lose_ot_total, lose_ot_pct,
          goals_for, goals_against, points,
          form, description,
          raw_json
        )
        VALUES (
          %s, %s, %s, %s,
          %s, %s,
          %s,
          %s, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s,
          %s, %s,
          %s::jsonb
        )
        ON CONFLICT (league_id, season, stage, group_name, team_id) DO UPDATE SET
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
          raw_json = EXCLUDED.raw_json;
        """

        tid = team.get("id")

        # --- [PATCH START] FK 방어: team_id가 0/None/비정상이면 스탠딩 스킵 ---
        if tid is None:
            log.warning("[standings skip] league=%s season=%s team_id is None row=%s", league_id, season, json_dumps(r))
            continue

        try:
            tid_i = int(tid)
        except Exception:
            log.warning("[standings skip] league=%s season=%s invalid team_id=%s row=%s", league_id, season, tid, json_dumps(r))
            continue

        if tid_i <= 0:
            log.warning("[standings skip] league=%s season=%s invalid team_id=%s row=%s", league_id, season, tid_i, json_dumps(r))
            continue

        # standings에서도 팀이 뜨니까 team upsert(국가 없을 수 있으니 country_id=None)
        upsert_team(team, None)
        # --- [PATCH END] ---

        execute(
            sql,
            (
                league_id,
                season,
                stage,
                group_name,
                tid_i,
                r.get("position"),
                games.get("played"),

                win.get("total"),
                num_or_none(win.get("percentage")),
                win_ot.get("total"),
                num_or_none(win_ot.get("percentage")),

                lose.get("total"),
                num_or_none(lose.get("percentage")),
                lose_ot.get("total"),
                num_or_none(lose_ot.get("percentage")),

                goals.get("for"),
                goals.get("against"),
                r.get("points"),

                r.get("form"),
                r.get("description"),
                json_dumps(r),
            ),
        )
        count += 1

    return count


# =========================
# Odds meta + odds rows
# (002에서 만든 hockey_odds_markets / hockey_odds_bookmakers / hockey_odds market_id/bookmaker_id)
# =========================
def upsert_odds_market(name: str) -> Optional[int]:
    if not name:
        return None
    sql = """
    INSERT INTO hockey_odds_markets (name)
    VALUES (%s)
    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
    RETURNING id;
    """
    row = fetch_one(sql, (name,))
    if row and "id" in row:
        return int(row["id"])
    # 혹시 RETURNING을 db헬퍼가 못받는 경우 대비
    row2 = fetch_one("SELECT id FROM hockey_odds_markets WHERE name=%s", (name,))
    return int(row2["id"]) if row2 else None

def upsert_odds_bookmaker(name: str) -> Optional[int]:
    if not name:
        return None
    sql = """
    INSERT INTO hockey_odds_bookmakers (name)
    VALUES (%s)
    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
    RETURNING id;
    """
    row = fetch_one(sql, (name,))
    if row and "id" in row:
        return int(row["id"])
    row2 = fetch_one("SELECT id FROM hockey_odds_bookmakers WHERE name=%s", (name,))
    return int(row2["id"]) if row2 else None

def upsert_odds_row(
    game_id: int,
    market_id: Optional[int],
    bookmaker_id: Optional[int],
    selection: Optional[str],
    odd_value: Optional[float],
    api_odds_id: Optional[str],
    provider: Optional[str],
    market_name_fallback: Optional[str],
    raw: Dict[str, Any],
    last_update: Optional[str] = None,
) -> None:
    sql = """
    INSERT INTO hockey_odds (
      game_id, api_odds_id, provider,
      market, selection, odd_value,
      market_id, bookmaker_id,
      last_update,
      raw_json
    )
    VALUES (
      %s, %s, %s,
      %s, %s, %s,
      %s, %s,
      %s,
      %s::jsonb
    )
    ON CONFLICT (game_id, market_id, bookmaker_id, selection) DO UPDATE SET
      api_odds_id = EXCLUDED.api_odds_id,
      provider = EXCLUDED.provider,
      market = EXCLUDED.market,
      odd_value = EXCLUDED.odd_value,
      last_update = EXCLUDED.last_update,
      raw_json = EXCLUDED.raw_json;
    """
    execute(
        sql,
        (
            game_id,
            api_odds_id,
            provider,
            market_name_fallback,
            selection,
            odd_value,
            market_id,
            bookmaker_id,
            last_update,
            json_dumps(raw),
        ),
    )


# =========================
# Events
# =========================
def insert_game_event(game_id: int, ev: Dict[str, Any], order_index: int) -> None:
    period = ev.get("period") or "UNK"
    minute = ev.get("minute")
    try:
        minute_i = int(str(minute).strip()) if minute is not None else None
    except Exception:
        minute_i = None

    team = ev.get("team") or {}
    team_id = team.get("id")

    ev_type = ev.get("type") or "unknown"
    comment = ev.get("comment")

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
      raw_json = EXCLUDED.raw_json;
    """
    execute(
        sql,
        (
            game_id,
            period,
            minute_i,
            team_id,
            ev_type,
            comment,
            players,
            assists,
            order_index,
            json_dumps(ev),
        ),
    )


# =========================
# main flow
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, help="예: 2025 (2025-2026 시즌 용도)")
    ap.add_argument("--league-id", required=True, help="예: 57 (NHL). 여러 개면 57,58,35")
    ap.add_argument("--sleep", type=float, default=0.25, help="API 호출간 sleep")
    ap.add_argument("--only-pages", type=int, default=0, help="테스트용: games page 몇 페이지만 돌릴지(0이면 전체)")
    ap.add_argument("--skip-events", action="store_true", help="events 스킵(기본은 포함)")
    ap.add_argument("--skip-odds", action="store_true", help="odds 스킵(기본은 포함)")
    args = ap.parse_args()

    api_key = (
        os.environ.get("APISPORTS_KEY")
        or os.environ.get("API_SPORTS_KEY")
        or os.environ.get("APIFOOTBALL_KEY")     # Render에 이미 있는 키명 fallback
        or os.environ.get("API_KEY")             # 혹시 공용 키명으로 쓰는 경우 대비
    )
    if not api_key:
        raise SystemExit("APISPORTS_KEY (또는 APIFOOTBALL_KEY) 환경변수 필요")


    season = int(str(args.season).strip())
    league_ids = [int(x.strip()) for x in str(args.league_id).split(",") if x.strip()]

    api = HockeyApi(api_key=api_key)

    # 1) leagues + seasons (선택한 league_id만)
    log.info("1) leagues + seasons upsert 시작 (season=%s, league_ids=%s)", season, league_ids)
    leagues_data = api.leagues()
    leagues_resp = leagues_data.get("response") or []

    # /leagues 응답이 flat item 구조임을 전제로 필터
    target_items: List[Dict[str, Any]] = []
    for item in leagues_resp:
        lid = item.get("id")
        if lid not in league_ids:
            continue

        # 해당 시즌이 seasons에 존재하는지 확인
        seasons_arr = item.get("seasons") or []
        seasons_in_item = {s.get("season") for s in seasons_arr if isinstance(s, dict)}
        if season not in seasons_in_item:
            log.warning("league_id=%s 에 season=%s 이 leagues.seasons에 없음(그래도 리그는 저장)", lid, season)

        target_items.append(item)

    if not target_items:
        raise SystemExit(f"선택한 league_id가 /leagues 에서 발견되지 않음: {league_ids}")

    for item in target_items:
        upsert_league_flat(item)
        for s in (item.get("seasons") or []):
            if isinstance(s, dict) and s.get("season") == season:
                upsert_league_season(int(item["id"]), s)
        time.sleep(args.sleep)

    # 2) games + teams
    for lid in league_ids:
        log.info("2) games + teams upsert 시작 league=%s season=%s", lid, season)
        page = 1
        total_games = 0
        game_ids: List[int] = []

        # leagues(country) id를 teams에 넣고 싶으면,
        # /games의 league.country가 없는 경우가 많아서 여기서는 None으로 둠.
        # teams.country_id가 꼭 필요하면 /teams endpoint 추가로 돌려야 함(원하면 해줄게).
                while True:
            g = api.games(league_id=lid, season=season)

            # --- [PATCH START] /games 응답 진단 로그 ---
            errs = g.get("errors")
            if errs:
                log.warning("[games errors] league=%s season=%s page=%s errors=%s full=%s", lid, season, page, errs, json_dumps(g))
                break

            resp = g.get("response") or []
            results = g.get("results")

            # ✅ [ADD] page=1부터 비어버리면 반드시 로그 남김 (지금 너 상황이 이 케이스일 확률 높음)
            if (not resp) and page == 1:
                log.warning("[games page1 empty] league=%s season=%s results=%s full=%s", lid, season, results, json_dumps(g))
                break

            if (not resp) and results not in (None, 0):
                # results가 있는데 resp가 비는 이상 케이스
                log.warning("[games empty response] league=%s season=%s page=%s results=%s full=%s", lid, season, page, results, json_dumps(g))
                break

            if not resp:
                # 완전 빈 경우(보통 끝 페이지)
                break
            # --- [PATCH END] ---


            for row in resp:
                teams = row.get("teams") or {}
                home = teams.get("home") or {}
                away = teams.get("away") or {}

                if home.get("id"):
                    upsert_team(home, None)
                if away.get("id"):
                    upsert_team(away, None)

                gid = upsert_game(row)
                if gid:
                    game_ids.append(gid)
                total_games += 1

            time.sleep(args.sleep)

            break

        log.info("league=%s season=%s games upserted=%d (unique game_ids=%d)", lid, season, total_games, len(set(game_ids)))

        # 3) standings
        try:
            st = api.standings(league_id=lid, season=season)
            cnt = upsert_standings(lid, season, st)
            log.info("3) standings upserted league=%s season=%s rows=%d", lid, season, cnt)
        except requests.HTTPError as e:
            log.warning("3) standings 없음/에러 league=%s season=%s (%s)", lid, season, str(e))

        time.sleep(args.sleep)


        # 4) events (모든 game_id 대상으로)
        if not args.skip_events:
            log.info("4) game_events upsert 시작 league=%s season=%s games=%d", lid, season, len(set(game_ids)))
            for gid in sorted(set(game_ids)):
                try:
                    ev = api.game_events(gid)
                    ev_resp = ev.get("response") or []
                    if not isinstance(ev_resp, list):
                        continue

                    for idx, item in enumerate(ev_resp):
                        if isinstance(item, dict):
                            # events 응답이 "event 객체 리스트"인 케이스
                            insert_game_event(gid, item, idx)
                        elif isinstance(item, list):
                            # 혹시 2중 배열이면 flatten
                            for j, inner in enumerate(item):
                                if isinstance(inner, dict):
                                    insert_game_event(gid, inner, j)
                    time.sleep(args.sleep)
                except requests.HTTPError as e:
                    # events 없는 경기 많음 -> 조용히 스킵
                    continue

        # 5) odds (모든 game_id 대상으로)
        if not args.skip_odds:
            log.info("5) odds upsert 시작 league=%s season=%s games=%d", lid, season, len(set(game_ids)))
            for gid in sorted(set(game_ids)):
                try:
                    od = api.odds(gid)
                    resp = od.get("response") or []
                    if not isinstance(resp, list) or not resp:
                        continue

                    # odds 응답 구조가 리그/플랜마다 다를 수 있어서 "최대한 안전" 파서:
                    # - market/bookmaker/values 같은 키가 있으면 정규화 저장
                    # - 없으면 raw_json만 hockey_odds에 넣는 fallback (market_id/bookmaker_id null)
                    for block in resp:
                        if not isinstance(block, dict):
                            continue

                        api_odds_id = str(block.get("id")) if block.get("id") is not None else None
                        last_update = block.get("update") or block.get("last_update") or block.get("updated_at")

                        bookmakers = block.get("bookmakers") or block.get("Bookmakers") or []
                        if isinstance(bookmakers, list) and bookmakers:
                            for b in bookmakers:
                                if not isinstance(b, dict):
                                    continue
                                bname = b.get("name") or b.get("bookmaker") or b.get("title")
                                bookmaker_id = upsert_odds_bookmaker(str(bname)) if bname else None

                                bets = b.get("bets") or b.get("markets") or b.get("Bets") or []
                                if not isinstance(bets, list) or not bets:
                                    # bookmaker는 있는데 시장이 없으면 raw로 1줄 저장
                                    upsert_odds_row(
                                        game_id=gid,
                                        market_id=None,
                                        bookmaker_id=bookmaker_id,
                                        selection=None,
                                        odd_value=None,
                                        api_odds_id=api_odds_id,
                                        provider=str(bname) if bname else None,
                                        market_name_fallback=None,
                                        raw=b,
                                        last_update=last_update,
                                    )
                                    continue

                                for m in bets:
                                    if not isinstance(m, dict):
                                        continue
                                    mname = m.get("name") or m.get("market") or m.get("label")
                                    market_id = upsert_odds_market(str(mname)) if mname else None

                                    values = m.get("values") or m.get("odds") or m.get("Values") or []
                                    if isinstance(values, list) and values:
                                        for v in values:
                                            if not isinstance(v, dict):
                                                continue
                                            selection = v.get("value") or v.get("name") or v.get("selection")
                                            oddv = v.get("odd") or v.get("price") or v.get("value_odd")
                                            odd_num = num_or_none(oddv)
                                            upsert_odds_row(
                                                game_id=gid,
                                                market_id=market_id,
                                                bookmaker_id=bookmaker_id,
                                                selection=str(selection) if selection is not None else None,
                                                odd_value=odd_num,
                                                api_odds_id=api_odds_id,
                                                provider=str(bname) if bname else None,
                                                market_name_fallback=str(mname) if mname else None,
                                                raw={"block": block, "bookmaker": b, "market": m, "value": v},
                                                last_update=last_update,
                                            )
                                    else:
                                        # 값 리스트가 없으면 시장 raw로 1줄
                                        upsert_odds_row(
                                            game_id=gid,
                                            market_id=market_id,
                                            bookmaker_id=bookmaker_id,
                                            selection=None,
                                            odd_value=None,
                                            api_odds_id=api_odds_id,
                                            provider=str(bname) if bname else None,
                                            market_name_fallback=str(mname) if mname else None,
                                            raw={"block": block, "bookmaker": b, "market": m},
                                            last_update=last_update,
                                        )
                        else:
                            # bookmakers 구조가 없으면 raw만 저장
                            upsert_odds_row(
                                game_id=gid,
                                market_id=None,
                                bookmaker_id=None,
                                selection=None,
                                odd_value=None,
                                api_odds_id=api_odds_id,
                                provider=None,
                                market_name_fallback=None,
                                raw=block,
                                last_update=last_update,
                            )

                    time.sleep(args.sleep)
                except requests.HTTPError:
                    continue

        log.info("✅ league=%s season=%s 백필 완료", lid, season)

    log.info("✅ 전체 백필 완료 (season=%s, league_ids=%s)", season, league_ids)


if __name__ == "__main__":
    main()

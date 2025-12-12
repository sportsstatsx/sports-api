from __future__ import annotations

import os
import time
import json
import argparse
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import execute, fetch_one

log = logging.getLogger("hockey_backfill")
logging.basicConfig(level=logging.INFO)


# ─────────────────────────────────────────
# API Client (API-Sports Hockey)
# ─────────────────────────────────────────
class HockeyApi:
    BASE_URL = "https://v1.hockey.api-sports.io"

    def __init__(self, api_key: str, *, timeout: int = 45):
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"x-apisports-key": self.api_key})

    def _get(self, path: str, params: Dict[str, Any], *, retries: int = 5) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        backoff = 1.0
        last_err: Optional[Exception] = None

        for attempt in range(1, retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)

                # API-Sports는 rate limit 때 429를 줄 수 있음
                if r.status_code == 429:
                    log.warning(
                        "429 rate limited. sleep=%.1fs (attempt %s/%s) url=%s params=%s",
                        backoff, attempt, retries, path, params
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)
                    continue

                r.raise_for_status()
                data = r.json()
                if not isinstance(data, dict):
                    raise ValueError(f"Unexpected JSON type: {type(data)}")
                return data

            except Exception as e:
                last_err = e
                log.warning(
                    "HTTP error. sleep=%.1fs (attempt %s/%s) url=%s params=%s err=%r",
                    backoff, attempt, retries, path, params, e
                )
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

        raise RuntimeError(
            f"API request failed after {retries} retries: {path} params={params} err={last_err!r}"
        )

    def league_by_id(self, league_id: int) -> Dict[str, Any]:
        # ✅ /leagues 전체 훑지 말고 id 조회
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


def as_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def as_list(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


# ─────────────────────────────────────────
# Upserts (001_init_hockey.sql 기준)
# ─────────────────────────────────────────
def upsert_country(country: Dict[str, Any]) -> None:
    """
    schema: hockey_countries(id PK, name NOT NULL, code, flag)
    API가 country를 dict로 주는 케이스만 처리
    """
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
    """
    schema: hockey_leagues(id PK, name NOT NULL, type NOT NULL, logo, country_id FK)
    """
    if not isinstance(league, dict):
        return
    lid = safe_int(league.get("id"))
    name = safe_text(league.get("name"))
    ltype = safe_text(league.get("type"))
    if lid is None or not name or not ltype:
        # 여기서 스킵되면 standings에서 FK 터짐 → 로그 남기고 리턴
        log.warning("league missing required fields: league=%s", league)
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


def upsert_league_season(league_id: int, s: Dict[str, Any]) -> None:
    """
    schema: hockey_league_seasons(league_id FK, season, current, start_date, end_date, PK(league_id, season))
    """
    if not isinstance(s, dict):
        return
    season = safe_int(s.get("season"))
    if season is None:
        return

    execute(
        """
        INSERT INTO hockey_league_seasons (league_id, season, current, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (league_id, season) DO UPDATE SET
          current = EXCLUDED.current,
          start_date = EXCLUDED.start_date,
          end_date = EXCLUDED.end_date
        """,
        (league_id, season, bool(s.get("current", False)), s.get("start"), s.get("end")),
    )


def upsert_team(team: Dict[str, Any]) -> None:
    """
    schema: hockey_teams(id PK, name NOT NULL, logo, country_id FK)
    API가 team.country를 dict로 주는 케이스만 처리
    """
    if not isinstance(team, dict):
        return
    tid = safe_int(team.get("id"))
    name = safe_text(team.get("name"))
    if tid is None or not name:
        return

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


def upsert_game(item: Dict[str, Any], league_id: int, season: int) -> Optional[int]:
    """
    schema: hockey_games(...)
    """
    if not isinstance(item, dict):
        return None

    g = item.get("game")
    if not isinstance(g, dict):
        g = item.get("games") if isinstance(item.get("games"), dict) else {}
    if not isinstance(g, dict):
        return None

    gid = safe_int(g.get("id"))
    if gid is None:
        return None

    league_obj = as_dict(item.get("league"))
    teams_obj = as_dict(item.get("teams"))
    scores_obj = as_dict(item.get("scores"))

    home = as_dict(teams_obj.get("home"))
    away = as_dict(teams_obj.get("away"))

    if safe_int(home.get("id")) is not None:
        upsert_team(home)
    if safe_int(away.get("id")) is not None:
        upsert_team(away)

    status_obj = as_dict(g.get("status"))
    status = safe_text(status_obj.get("short"))
    status_long = safe_text(status_obj.get("long"))

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
            g.get("date"),
            status,
            status_long,
            safe_text(g.get("timezone")),
            json.dumps(scores_obj, ensure_ascii=False),
            json.dumps(item, ensure_ascii=False),
        ),
    )
    return gid


def insert_event_nodup(game_id: int, ev: Dict[str, Any], order: int) -> None:
    """
    events 중복 방지(스키마 변경 없이):
    같은 (game_id, period, minute, team_id, type, event_order) 조합이 이미 있으면 INSERT 안 함.
    """
    if not isinstance(ev, dict):
        return

    period = safe_text(ev.get("period")) or "UNK"
    minute = safe_int(ev.get("minute"))
    team = as_dict(ev.get("team"))
    team_id = safe_int(team.get("id"))
    etype = safe_text(ev.get("type")) or "unknown"
    comment = safe_text(ev.get("comment"))

    players = as_list(ev.get("players"))
    assists = as_list(ev.get("assists"))
    players = [safe_text(x) for x in players if safe_text(x)]
    assists = [safe_text(x) for x in assists if safe_text(x)]

    # team_id FK 때문에: team_id가 있으면 teams에 upsert 먼저 (없는 경우는 NULL로 저장)
    if team_id is not None:
        upsert_team(team)

    execute(
        """
        INSERT INTO hockey_game_events (
          game_id, period, minute, team_id,
          type, comment, players, assists,
          event_order, raw_json
        )
        SELECT
          %s,%s,%s,%s,
          %s,%s,%s,%s,
          %s,%s::jsonb
        WHERE NOT EXISTS (
          SELECT 1
          FROM hockey_game_events e
          WHERE e.game_id = %s
            AND e.period = %s
            AND ( (e.minute IS NOT DISTINCT FROM %s) )
            AND ( (e.team_id IS NOT DISTINCT FROM %s) )
            AND e.type = %s
            AND e.event_order = %s
        )
        """,
        (
            game_id, period, minute, team_id,
            etype, comment, players, assists,
            order, json.dumps(ev, ensure_ascii=False),

            game_id, period, minute, team_id, etype, order,
        ),
    )


def parse_standings_blocks(payload: Dict[str, Any]) -> List[Tuple[Optional[str], Optional[str], List[List[Dict[str, Any]]]]]:
    """
    API-Sports standings 응답은 리그/시즌에 따라 형태가 달라질 수 있어서,
    최대한 안전하게 "블록(리스트의 리스트)"를 추출한다.

    반환: [(stage, group_name, blocks)] 형태의 리스트
      - blocks: [ [row,row,...], [row,row,...], ... ]
    """
    out: List[Tuple[Optional[str], Optional[str], List[List[Dict[str, Any]]]]] = []
    resp = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(resp, list):
        return out

    for item in resp:
        if isinstance(item, dict) and "standings" in item:
            stage = safe_text(item.get("stage"))
            group_name = safe_text(item.get("group"))
            blocks_raw = item.get("standings")
            blocks: List[List[Dict[str, Any]]] = []
            for b in as_list(blocks_raw):
                if isinstance(b, list):
                    rows = [r for r in b if isinstance(r, dict)]
                    if rows:
                        blocks.append(rows)
            if blocks:
                out.append((stage, group_name, blocks))
        elif isinstance(item, list):
            # 어떤 리그는 response가 바로 [[...]] 형태로 오는 케이스가 있음
            blocks: List[List[Dict[str, Any]]] = []
            for b in item:
                if isinstance(b, list):
                    rows = [r for r in b if isinstance(r, dict)]
                    if rows:
                        blocks.append(rows)
            if blocks:
                out.append((None, None, blocks))

    return out


def upsert_standings(league_id: int, season: int, payload: Dict[str, Any]) -> int:
    """
    schema: hockey_standings PK(league_id, season, stage, group_name, team_id)
    → stage/group_name/team_id는 NOT NULL이 됨 (PK 포함 컬럼)
    따라서 누락 시 기본값을 넣거나 row를 스킵해야 함.

    returns: inserted/updated rows count(대략)
    """
    # FK 안전장치: league가 실제로 존재해야 함
    chk_league = fetch_one("SELECT id FROM hockey_leagues WHERE id=%s", (league_id,))
    if not chk_league:
        log.error("❌ standings blocked: league not found in hockey_leagues. league_id=%s", league_id)
        return 0

    total = 0
    groups = parse_standings_blocks(payload)
    if not groups:
        return 0

    for stage_hint, group_hint, blocks in groups:
        for block in blocks:
            for row in block:
                team = as_dict(row.get("team"))
                team_id = safe_int(team.get("id"))

                # ✅ team_id가 없거나 0이면 FK 깨짐 → 스킵
                if not team_id or team_id <= 0:
                    continue

                upsert_team(team)

                stage = safe_text(row.get("stage")) or stage_hint or "ALL"

                # group은 dict/문자열 둘 다 올 수 있음
                g = row.get("group")
                group_name = None
                if isinstance(g, dict):
                    group_name = safe_text(g.get("name"))
                else:
                    group_name = safe_text(g)
                group_name = group_name or group_hint or "ALL"

                position = safe_int(row.get("position")) or safe_int(row.get("rank"))
                if position is None:
                    continue

                win = as_dict(row.get("win"))
                win_ot = as_dict(row.get("win_overtime"))
                lose = as_dict(row.get("lose"))
                lose_ot = as_dict(row.get("lose_overtime"))
                goals = as_dict(row.get("goals"))
                games = as_dict(row.get("games"))

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
                        position,

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
                        json.dumps(row, ensure_ascii=False),
                    ),
                )
                total += 1

    return total


def is_future_game(game_id: int) -> bool:
    row = fetch_one("SELECT status, game_date FROM hockey_games WHERE id=%s", (game_id,))
    if not row:
        return False

    st = row.get("status")
    gd = row.get("game_date")

    # 최소한의 future 필터
    if st in ("NS", "TBD"):
        return True

    if gd is None:
        return False

    chk = fetch_one("SELECT (%s::timestamptz > NOW()) AS is_future", (gd,))
    return bool(chk and chk.get("is_future"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--league-id", required=True)  # comma-separated
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--skip-events", action="store_true")
    ap.add_argument("--skip-standings", action="store_true")
    ap.add_argument("--skip-future-events", action="store_true")
    args = ap.parse_args()

    api_key = (
        os.environ.get("APISPORTS_KEY")
        or os.environ.get("API_SPORTS_KEY")
        or os.environ.get("APISPORTS_HOCKEY_KEY")
    )
    if not api_key:
        raise SystemExit("APISPORTS_KEY (or API_SPORTS_KEY) is not set")

    api = HockeyApi(api_key)
    season = args.season
    league_ids = [int(x.strip()) for x in args.league_id.split(",") if x.strip()]

    # 1) league meta upsert (응답 형태 A/B 모두 지원)
    for lid in league_ids:
        meta = api.league_by_id(lid)
        resp = meta.get("response") if isinstance(meta, dict) else None
        if not isinstance(resp, list) or not resp or not isinstance(resp[0], dict):
            log.error("❌ league meta empty/invalid: league_id=%s meta=%s", lid, meta)
            raise SystemExit(2)

        item = resp[0]

        # 형태 A: {"league": {...}, "country": {...}, "seasons":[...]}
        if isinstance(item.get("league"), dict):
            league_obj = as_dict(item.get("league"))
            country_obj = item.get("country") if isinstance(item.get("country"), dict) else None
            seasons_obj = as_list(item.get("seasons"))
        else:
            # 형태 B: item 자체가 league 객체
            league_obj = as_dict(item)
            country_obj = item.get("country") if isinstance(item.get("country"), dict) else None
            seasons_obj = as_list(item.get("seasons"))

        upsert_league(league_obj, country_obj)
        for s in seasons_obj:
            if isinstance(s, dict):
                upsert_league_season(lid, s)

        # ✅ FK 터지기 전에 확인
        chk = fetch_one("SELECT id FROM hockey_leagues WHERE id=%s", (lid,))
        if not chk:
            log.error("❌ league upsert failed: league_id=%s item=%s", lid, item)
            raise SystemExit(2)

    # 2) games -> events -> standings
    for lid in league_ids:
        games_payload = api.games(lid, season)
        resp = games_payload.get("response") if isinstance(games_payload, dict) else None
        if not isinstance(resp, list):
            log.error("❌ games response invalid: league_id=%s season=%s payload=%s", lid, season, games_payload)
            raise SystemExit(3)

        game_ids: List[int] = []
        for item in resp:
            gid = upsert_game(item if isinstance(item, dict) else {}, lid, season)
            if gid:
                game_ids.append(gid)

        log.info("league=%s season=%s games_upserted=%s", lid, season, len(game_ids))

        # events
        if not args.skip_events:
            for gid in game_ids:
                if args.skip_future_events and is_future_game(gid):
                    continue

                ev_payload = api.game_events(gid)
                ev_list = ev_payload.get("response") if isinstance(ev_payload, dict) else None
                if isinstance(ev_list, list):
                    for idx, ev in enumerate(ev_list):
                        insert_event_nodup(gid, ev if isinstance(ev, dict) else {}, idx)

                if args.sleep > 0:
                    time.sleep(args.sleep)

        # standings
        if not args.skip_standings:
            st_payload = api.standings(lid, season)
            n = upsert_standings(lid, season, st_payload)
            log.info("league=%s season=%s standings_upserted=%s", lid, season, n)

    log.info("✅ hockey backfill complete")


if __name__ == "__main__":
    main()

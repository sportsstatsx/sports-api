# postmatch_backfill.py
#
# 역할:
# - 특정 날짜(date=YYYY-MM-DD)의 FINISHED 경기들에 대해 "한 번만" 무거운 데이터 전체 백필
#   * /fixtures          → fixtures/matches upsert + match_fixtures_raw 저장
#   * /fixtures/events   → match_events / match_events_raw
#   * /fixtures/lineups  → match_lineups
#   * /fixtures/statistics → match_team_stats
#   * /fixtures/players  → match_player_stats
#   * /standings         → standings (league+season)
#
# 특징:

# - 이미 백필된 경기(match_events에 row 존재)는 스킵
# - LIVE_LEAGUES env 에 포함된 리그만 대상
# - 스키마 변경 없음

import os
import sys
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional

import requests

from db import fetch_one, fetch_all, execute

BASE_URL = "https://v3.football.api-sports.io"


# ─────────────────────────────────────
#  ENV / 유틸
# ─────────────────────────────────────

def _get_api_key() -> str:
    key = (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_KEY")
        or ""
    )
    if not key:
        raise RuntimeError("API key missing: set APIFOOTBALL_KEY (or API_FOOTBALL_KEY / API_KEY)")
    return key


def _get_headers() -> Dict[str, str]:
    return {"x-apisports-key": _get_api_key()}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_live_leagues(s: str) -> List[int]:
    out: List[int] = []
    for tok in (s or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            out.append(int(tok))
        except ValueError:
            print(f"[WARN] LIVE_LEAGUES token invalid: {tok!r}", file=sys.stderr)
    return sorted(set(out))


def get_target_date() -> str:
    # 우선순위: ENV TARGET_DATE > CLI arg1 > 오늘(UTC)
    env_date = (os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return env_date
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        return sys.argv[1].strip()
    return now_utc().strftime("%Y-%m-%d")

def get_target_dates() -> List[str]:
    # 우선순위: ENV TARGET_DATE > CLI arg1 > 최근 N일(기본 7일)
    env_date = (os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return [env_date]
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        return [sys.argv[1].strip()]

    days = int((os.environ.get("BACKFILL_DAYS") or "7").strip())
    if days <= 0:
        days = 7

    # ✅ 크론 기준 "오늘"을 KST 기준으로 계산 (UTC 기준 하루 밀림 방지)
    kst = dt.timezone(dt.timedelta(hours=9))
    end = now_utc().astimezone(kst).date()
    start = end - dt.timedelta(days=days - 1)

    out: List[str] = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += dt.timedelta(days=1)
    return out

def get_target_season() -> Optional[int]:
    """
    시즌 모드:
      - ENV TARGET_SEASON=2024
      - 또는 CLI: --season 2024
    """
    env_season = (os.environ.get("TARGET_SEASON") or "").strip()
    if env_season:
        try:
            return int(env_season)
        except ValueError:
            print(f"[WARN] TARGET_SEASON invalid: {env_season!r}", file=sys.stderr)
            return None

    # CLI: --season 2024
    if "--season" in sys.argv:
        try:
            idx = sys.argv.index("--season")
            if idx + 1 < len(sys.argv):
                return int(sys.argv[idx + 1].strip())
        except Exception:
            print("[WARN] --season value invalid", file=sys.stderr)
            return None

    return None


def fetch_fixtures_for_season(league_id: int, season: int) -> List[Dict[str, Any]]:
    """
    시즌 전체 백필용 fixtures 수집.

    - FINISHED 범주(FT/AET/PEN)를 최대한 커버한다.
    - API가 status 콤마를 허용하면 1콜로 끝내고,
      거부하면 FT/AET/PEN을 분해 호출해서 합친다.
    """
    tz = (os.environ.get("API_TZ") or "Asia/Seoul").strip()

    finished_statuses = (os.environ.get("FINISHED_STATUSES") or "FT,AET,PEN").strip()
    statuses = [s.strip().upper() for s in finished_statuses.split(",") if s.strip()]
    if not statuses:
        statuses = ["FT", "AET", "PEN"]

    base_params: Dict[str, Any] = {
        "league": int(league_id),
        "season": int(season),
        "timezone": tz,
    }

    merged: Dict[int, Dict[str, Any]] = {}

    # 1) 콤마로 1번에 시도
    try:
        params = dict(base_params)
        params["status"] = ",".join(statuses)
        data = _safe_get("/fixtures", params=params)
        rows = data.get("response", []) or []
        for r in rows:
            if not isinstance(r, dict):
                continue
            basic = _extract_fixture_basic(r)
            if not basic:
                continue
            merged[basic["fixture_id"]] = r

        if merged:
            return list(merged.values())
    except Exception:
        pass

    # 2) fallback: status를 분해해서 여러 번 호출 후 merge
    for st in statuses:
        try:
            params = dict(base_params)
            params["status"] = st
            data = _safe_get("/fixtures", params=params)
            rows = data.get("response", []) or []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                basic = _extract_fixture_basic(r)
                if not basic:
                    continue
                merged[basic["fixture_id"]] = r
        except Exception as e:
            print(
                f"[WARN] fixtures season fetch failed league={league_id} season={season} status={st}: {e}",
                file=sys.stderr,
            )
            continue

    return list(merged.values())



def _safe_get(path: str, *, params: Dict[str, Any], timeout: int = 25, max_retry: int = 4) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    last_err: Optional[Exception] = None
    for i in range(max_retry):
        try:
            resp = requests.get(url, headers=_get_headers(), params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.7 * (i + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("API response is not a dict")

            # ✅ 200 OK라도 errors가 차있을 수 있음(예: season required)
            errs = data.get("errors")
            if isinstance(errs, dict) and errs:
                print(f"[WARN] API errors on {path} params={params}: {errs}", file=sys.stderr)
            elif isinstance(errs, list) and len(errs) > 0:
                print(f"[WARN] API errors on {path} params={params}: {errs}", file=sys.stderr)

            return data
        except Exception as e:
            last_err = e
            time.sleep(0.7 * (i + 1))
            continue
    raise RuntimeError(f"API request failed after retries: {last_err}")



def _status_group_from_short(short: Optional[str]) -> str:
    s = (short or "").upper().strip()

    if s in ("FT", "AET", "PEN"):
        return "FINISHED"

    # 라이브 워커와 동일하게 맞춤
    if s in ("NS", "TBD"):
        return "UPCOMING"

    # INPLAY(HT 포함)
    if s in ("1H", "2H", "ET", "P", "BT", "INT", "LIVE", "HT"):
        return "INPLAY"

    # 연기/취소/중단 등
    if s in ("PST", "CANC", "ABD", "AWD", "WO", "SUSP"):
        return "OTHER"

    return "OTHER"



def _extract_fixture_basic(fx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fixture = fx.get("fixture") or {}
    league = fx.get("league") or {}
    teams = fx.get("teams") or {}

    fid = fixture.get("id")
    if fid is None:
        return None

    status = fixture.get("status") or {}
    status_short = status.get("short")
    status_group = _status_group_from_short(status_short)

    season = league.get("season")
    league_id = league.get("id")

    home_id = (teams.get("home") or {}).get("id")
    away_id = (teams.get("away") or {}).get("id")

    return {
        "fixture_id": int(fid),
        "league_id": int(league_id) if league_id is not None else None,
        "season": int(season) if season is not None else None,
        "status_short": status_short,
        "status_group": status_group,
        "home_id": int(home_id) if home_id is not None else None,
        "away_id": int(away_id) if away_id is not None else None,
    }


# ─────────────────────────────────────
#  API fetchers
# ─────────────────────────────────────

def fetch_league_seasons(league_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/leagues", params={"id": league_id})
    resp = data.get("response") or []
    if not resp or not isinstance(resp, list) or not isinstance(resp[0], dict):
        return []
    seasons = (resp[0].get("seasons") or [])
    return [s for s in seasons if isinstance(s, dict)]


def pick_season_for_date(league_id: int, date_str: str) -> Optional[int]:
    # date_str: "YYYY-MM-DD"
    seasons = fetch_league_seasons(league_id)
    if not seasons:
        return None

    # 1) start <= date <= end 범위 매칭 우선
    for s in seasons:
        start = s.get("start")
        end = s.get("end")
        year = s.get("year")
        if start and end and year is not None and start <= date_str <= end:
            try:
                return int(year)
            except Exception:
                return None

    # 2) 범위 매칭 실패 시 current=True 시즌으로 fallback
    for s in seasons:
        if s.get("current") is True and s.get("year") is not None:
            try:
                return int(s["year"])
            except Exception:
                return None

    return None


def fetch_fixtures_from_api(league_id: int, date_str: str, season: Optional[int] = None) -> List[Dict[str, Any]]:
    if season is None:
        season = pick_season_for_date(league_id, date_str)

    # ✅ date 필터가 timezone 영향을 받으니 반드시 포함
    tz = (os.environ.get("API_TZ") or "Asia/Seoul").strip()

    params: Dict[str, Any] = {"league": league_id, "date": date_str, "timezone": tz}
    if season is not None:
        params["season"] = int(season)

    data = _safe_get("/fixtures", params=params)
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]




def fetch_fixture_by_id(fixture_id: int) -> Optional[Dict[str, Any]]:
    data = _safe_get("/fixtures", params={"id": fixture_id})
    rows = data.get("response", []) or []
    for r in rows:
        if isinstance(r, dict):
            return r
    return None


def fetch_events_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/fixtures/events", params={"fixture": fixture_id})
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]


def fetch_lineups_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/fixtures/lineups", params={"fixture": fixture_id})
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]


def fetch_team_stats_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/fixtures/statistics", params={"fixture": fixture_id})
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]


def fetch_player_stats_from_api(fixture_id: int) -> List[Dict[str, Any]]:
    data = _safe_get("/fixtures/players", params={"fixture": fixture_id})
    rows = data.get("response", []) or []
    return [r for r in rows if isinstance(r, dict)]

def fetch_standings_from_api(league_id: int, season: int) -> List[Dict[str, Any]]:
    # API-Sports standings는 보통 response[0].league.standings 가 "리스트의 리스트" 구조
    data = _safe_get("/standings", params={"league": league_id, "season": int(season)})
    resp = data.get("response") or []
    if not resp or not isinstance(resp, list) or not isinstance(resp[0], dict):
        return []

    league = (resp[0].get("league") or {})
    standings = league.get("standings") or []
    if not isinstance(standings, list):
        return []

    out: List[Dict[str, Any]] = []
    for group in standings:
        # group: 보통 List[Dict] (그룹/컨퍼런스/Overall 등)
        if isinstance(group, list):
            for row in group:
                if isinstance(row, dict):
                    out.append(row)
        elif isinstance(group, dict):
            # 혹시 단일 dict로 오는 예외 케이스
            out.append(group)
    return out



# ─────────────────────────────────────
#  DB upserts (스키마 그대로)
# ─────────────────────────────────────

def upsert_match_fixtures_raw(fixture_id: int, fixture_obj: Dict[str, Any]) -> None:
    raw = json.dumps(fixture_obj, ensure_ascii=False)
    execute(
        """
        INSERT INTO match_fixtures_raw (fixture_id, data_json, fetched_at, updated_at)
        VALUES (%s, %s, now(), now())
        ON CONFLICT (fixture_id) DO UPDATE
        SET data_json = EXCLUDED.data_json,
            fetched_at = now(),
            updated_at = now()
        """,
        (fixture_id, raw),
    )


def upsert_fixture_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    fixture_block = fx.get("fixture") or {}
    fid = fixture_block.get("id")
    if fid is None:
        return

    date_utc = fixture_block.get("date")
    status_short = (fixture_block.get("status") or {}).get("short")
    status_group = _status_group_from_short(status_short)

    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, season, date_utc, status, status_group)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id     = EXCLUDED.league_id,
            season        = EXCLUDED.season,
            date_utc      = EXCLUDED.date_utc,
            status        = EXCLUDED.status,
            status_group  = EXCLUDED.status_group
        """,
        (int(fid), int(league_id), int(season), date_utc, status_short, status_group),
    )


def upsert_match_row(fx: Dict[str, Any], league_id: int, season: int) -> None:
    fixture_block = fx.get("fixture") or {}
    teams_block = fx.get("teams") or {}
    goals_block = fx.get("goals") or {}
    score_block = fx.get("score") or {}

    fid = fixture_block.get("id")
    if fid is None:
        return

    date_utc = fixture_block.get("date")
    st = fixture_block.get("status") or {}
    status_short = st.get("short")
    status_long = st.get("long")
    status_elapsed = st.get("elapsed")
    status_extra = st.get("extra")
    status_group = _status_group_from_short(status_short)

    home_id = (teams_block.get("home") or {}).get("id")
    away_id = (teams_block.get("away") or {}).get("id")
    if home_id is None or away_id is None:
        return

    home_ft = goals_block.get("home")
    away_ft = goals_block.get("away")

    ht = score_block.get("halftime") or {}
    home_ht = ht.get("home")
    away_ht = ht.get("away")

    elapsed = status_elapsed

    referee = fixture_block.get("referee")
    fixture_timezone = fixture_block.get("timezone")
    fixture_timestamp = fixture_block.get("timestamp")
    venue = fixture_block.get("venue") or {}
    venue_id = venue.get("id")
    venue_name = venue.get("name")
    venue_city = venue.get("city")

    league_round = (fx.get("league") or {}).get("round")

    execute(
        """
        INSERT INTO matches (
            fixture_id, league_id, season, date_utc, status, status_group,
            home_id, away_id,
            home_ft, away_ft, elapsed,
            home_ht, away_ht,
            referee, fixture_timezone, fixture_timestamp,
            status_short, status_long, status_elapsed, status_extra,
            venue_id, venue_name, venue_city, league_round
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id = EXCLUDED.league_id,
            season = EXCLUDED.season,
            date_utc = EXCLUDED.date_utc,
            status = EXCLUDED.status,
            status_group = EXCLUDED.status_group,
            home_id = EXCLUDED.home_id,
            away_id = EXCLUDED.away_id,
            home_ft = EXCLUDED.home_ft,
            away_ft = EXCLUDED.away_ft,
            elapsed = EXCLUDED.elapsed,
            home_ht = EXCLUDED.home_ht,
            away_ht = EXCLUDED.away_ht,
            referee = EXCLUDED.referee,
            fixture_timezone = EXCLUDED.fixture_timezone,
            fixture_timestamp = EXCLUDED.fixture_timestamp,
            status_short = EXCLUDED.status_short,
            status_long = EXCLUDED.status_long,
            status_elapsed = EXCLUDED.status_elapsed,
            status_extra = EXCLUDED.status_extra,
            venue_id = EXCLUDED.venue_id,
            venue_name = EXCLUDED.venue_name,
            venue_city = EXCLUDED.venue_city,
            league_round = EXCLUDED.league_round
        """,
        (
            int(fid), int(league_id), int(season), date_utc, status_short, status_group,
            int(home_id), int(away_id),
            home_ft, away_ft, elapsed,
            home_ht, away_ht,
            referee, fixture_timezone, fixture_timestamp,
            status_short, status_long, status_elapsed, status_extra,
            venue_id, venue_name, venue_city, league_round,
        ),
    )


def upsert_match_events_raw(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    execute(
        """
        INSERT INTO match_events_raw (fixture_id, data_json)
        VALUES (%s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            data_json = EXCLUDED.data_json
        """,
        (fixture_id, json.dumps(events, ensure_ascii=False)),
    )


def upsert_match_events(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    # postmatch는 최종본 목적 → fixture 단위 통째 덮어쓰기
    execute("DELETE FROM match_events WHERE fixture_id = %s", (fixture_id,))

    for ev in events:
        if not isinstance(ev, dict):
            continue

        team_id = (ev.get("team") or {}).get("id")
        player_id = (ev.get("player") or {}).get("id")
        assist_id = (ev.get("assist") or {}).get("id")
        assist_name = (ev.get("assist") or {}).get("name")

        type_ = ev.get("type") or ""
        if not type_:
            continue

        detail = ev.get("detail")
        time_block = ev.get("time") or {}
        minute = time_block.get("elapsed")
        extra = time_block.get("extra")

        # minute NOT NULL
        if minute is None:
            continue

        # Subst: 들어온 선수는 assist쪽에 실리는 경우가 많음
        player_in_id = None
        player_in_name = None
        if str(type_).lower() == "subst":
            player_in_id = assist_id
            player_in_name = assist_name

        execute(
            """
            INSERT INTO match_events (
                fixture_id,
                team_id,
                player_id,
                type,
                detail,
                minute,
                extra,
                assist_player_id,
                assist_name,
                player_in_id,
                player_in_name
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                fixture_id,
                team_id,
                player_id,
                type_,
                detail,
                int(minute),
                extra,
                assist_id,
                assist_name,
                player_in_id,
                player_in_name,
            ),
        )


def upsert_match_lineups(fixture_id: int, lineups: List[Dict[str, Any]]) -> None:
    execute("DELETE FROM match_lineups WHERE fixture_id = %s", (fixture_id,))
    updated_utc = now_utc().isoformat()

    for row in lineups:
        if not isinstance(row, dict):
            continue
        team_id = (row.get("team") or {}).get("id")
        if team_id is None:
            continue
        execute(
            """
            INSERT INTO match_lineups (fixture_id, team_id, data_json, updated_utc)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                data_json = EXCLUDED.data_json,
                updated_utc = EXCLUDED.updated_utc
            """,
            (fixture_id, int(team_id), json.dumps(row, ensure_ascii=False), updated_utc),
        )


def upsert_match_team_stats(fixture_id: int, stats: List[Dict[str, Any]]) -> None:
    execute("DELETE FROM match_team_stats WHERE fixture_id = %s", (fixture_id,))

    for row in stats:
        if not isinstance(row, dict):
            continue
        team_id = (row.get("team") or {}).get("id")
        if team_id is None:
            continue
        stat_list = row.get("statistics") or []
        for s in stat_list:
            if not isinstance(s, dict):
                continue
            name = s.get("type")
            if not name:
                continue
            value = s.get("value")
            value_str = None if value is None else str(value)

            execute(
                """
                INSERT INTO match_team_stats (fixture_id, team_id, name, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fixture_id, team_id, name) DO UPDATE SET
                    value = EXCLUDED.value
                """,
                (fixture_id, int(team_id), str(name), value_str),
            )


def upsert_match_player_stats(fixture_id: int, players_stats: List[Dict[str, Any]]) -> None:
    execute("DELETE FROM match_player_stats WHERE fixture_id = %s", (fixture_id,))

    for team_block in players_stats:
        if not isinstance(team_block, dict):
            continue
        players_list = team_block.get("players") or []
        for p in players_list:
            if not isinstance(p, dict):
                continue
            player_id = (p.get("player") or {}).get("id")
            if player_id is None:
                continue
            execute(
                """
                INSERT INTO match_player_stats (fixture_id, player_id, data_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (fixture_id, player_id) DO UPDATE SET
                    data_json = EXCLUDED.data_json
                """,
                (fixture_id, int(player_id), json.dumps(p, ensure_ascii=False)),
            )

def upsert_standings_rows(league_id: int, season: int, rows: List[Dict[str, Any]]) -> None:
    # standings는 league+season 단위 최종본 목적 → 통째로 교체(스테일 팀/그룹 제거)
    execute("DELETE FROM standings WHERE league_id = %s AND season = %s", (int(league_id), int(season)))

    updated_utc = now_utc().isoformat()

    for r in rows:
        if not isinstance(r, dict):
            continue

        rank = r.get("rank")
        team_id = (r.get("team") or {}).get("id")

        # NOT NULL 컬럼들 방어
        if rank is None or team_id is None:
            continue

        group_name = (r.get("group") or "Overall")
        points = r.get("points")
        goals_diff = r.get("goalsDiff")

        all_ = r.get("all") or {}
        played = all_.get("played")
        win = all_.get("win")
        draw = all_.get("draw")
        lose = all_.get("lose")

        goals = all_.get("goals") or {}
        goals_for = goals.get("for")
        goals_against = goals.get("against")

        form = r.get("form")
        description = r.get("description")

        execute(
            """
            INSERT INTO standings (
                league_id, season, group_name, rank, team_id,
                points, goals_diff, played, win, draw, lose,
                goals_for, goals_against, form, updated_utc, description
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (league_id, season, group_name, team_id) DO UPDATE SET
                rank = EXCLUDED.rank,
                points = EXCLUDED.points,
                goals_diff = EXCLUDED.goals_diff,
                played = EXCLUDED.played,
                win = EXCLUDED.win,
                draw = EXCLUDED.draw,
                lose = EXCLUDED.lose,
                goals_for = EXCLUDED.goals_for,
                goals_against = EXCLUDED.goals_against,
                form = EXCLUDED.form,
                updated_utc = EXCLUDED.updated_utc,
                description = EXCLUDED.description
            """,
            (
                int(league_id), int(season), str(group_name), int(rank), int(team_id),
                points, goals_diff, played, win, draw, lose,
                goals_for, goals_against, form, updated_utc, description
            ),
        )



# ─────────────────────────────────────
#  이미 백필된 경기인지 체크
# ─────────────────────────────────────

def _has_any_row(table: str, fixture_id: int) -> bool:
    row = fetch_one(
        f"""
        SELECT 1
        FROM {table}
        WHERE fixture_id = %s
        LIMIT 1
        """,
        (fixture_id,),
    )
    return row is not None


def has_match_events(fixture_id: int) -> bool:
    return _has_any_row("match_events", fixture_id)


def has_lineups(fixture_id: int) -> bool:
    return _has_any_row("match_lineups", fixture_id)


def has_team_stats(fixture_id: int) -> bool:
    return _has_any_row("match_team_stats", fixture_id)


def has_player_stats(fixture_id: int) -> bool:
    return _has_any_row("match_player_stats", fixture_id)

def has_standings(league_id: int, season: int) -> bool:
    row = fetch_one(
        """
        SELECT 1
        FROM standings
        WHERE league_id = %s AND season = %s
        LIMIT 1
        """,
        (int(league_id), int(season)),
    )
    return row is not None




# ─────────────────────────────────────
#  한 경기 상세 백필
# ─────────────────────────────────────

def backfill_postmatch_for_fixture(
    fixture_id: int,
    *,
    do_events: bool,
    do_lineups: bool,
    do_team_stats: bool,
    do_player_stats: bool,
) -> None:
    if do_events:
        try:
            events = fetch_events_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: events 호출 에러: {e}", file=sys.stderr)
            events = []
        if events:
            upsert_match_events(fixture_id, events)
            upsert_match_events_raw(fixture_id, events)

    if do_lineups:
        try:
            lineups = fetch_lineups_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: lineups 호출 에러: {e}", file=sys.stderr)
            lineups = []
        if lineups:
            upsert_match_lineups(fixture_id, lineups)

    if do_team_stats:
        try:
            stats = fetch_team_stats_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: statistics 호출 에러: {e}", file=sys.stderr)
            stats = []
        if stats:
            upsert_match_team_stats(fixture_id, stats)

    if do_player_stats:
        try:
            players_stats = fetch_player_stats_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: players 호출 에러: {e}", file=sys.stderr)
            players_stats = []
        if players_stats:
            upsert_match_player_stats(fixture_id, players_stats)



# ─────────────────────────────────────
#  엔트리
# ─────────────────────────────────────

def main() -> None:
    target_season = get_target_season()
    target_dates = get_target_dates()  # 시즌 모드가 아니면 기존대로 날짜 모드 사용
    live_leagues = parse_live_leagues(os.environ.get("LIVE_LEAGUES", ""))

    if not live_leagues:
        print("[postmatch_backfill] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return

    force = (os.environ.get("FORCE_BACKFILL") or "").strip().lower() in ("1", "true", "yes", "y")
    today_str = now_utc().strftime("%Y-%m-%d")

    if target_season is not None:
        print(f"[postmatch_backfill] season_mode season={target_season}, today={today_str}, leagues={live_leagues}, force={force}")
    else:
        print(f"[postmatch_backfill] dates={target_dates}, today={today_str}, leagues={live_leagues}, force={force}")


    total_new = 0
    total_skipped = 0

    fetched_standings_keys = set()  # (league_id, season) 중복 호출 방지

    # ─────────────────────────────────────
    #  ✅ 시즌 모드: league + season 전체(FT) 백필
    # ─────────────────────────────────────
    if target_season is not None:
        for lid in live_leagues:
            try:
                # standings (league+season)
                skey = (int(lid), int(target_season))
                need_st = force or (not has_standings(int(lid), int(target_season)))
                if need_st and skey not in fetched_standings_keys:
                    try:
                        st_rows = fetch_standings_from_api(int(lid), int(target_season))
                        if st_rows:
                            upsert_standings_rows(int(lid), int(target_season), st_rows)
                            fetched_standings_keys.add(skey)
                            print(f"    * standings league={lid} season={target_season}: rows={len(st_rows)}")
                        else:
                            print(f"    ! standings league={lid} season={target_season}: empty response", file=sys.stderr)
                    except Exception as se:
                        print(f"    ! standings league={lid} season={target_season} 에러: {se}", file=sys.stderr)

                fixtures = fetch_fixtures_for_season(int(lid), int(target_season))
                print(f"  - season={target_season} league {lid}: fixtures={len(fixtures)}")

                for fx in fixtures:
                    basic = _extract_fixture_basic(fx)
                    if basic is None:
                        continue

                    fixture_id = basic["fixture_id"]
                    sg = (basic.get("status_group") or "").strip()

                    # ✅ 시즌 모드에서는 season을 target_season으로 고정
                    season = int(target_season)

                    # ✅ 모든 상태에서 fixtures/matches/raw 업서트 (기존 정책 유지)
                    fx_full = fetch_fixture_by_id(fixture_id) or fx

                    try:
                        upsert_match_fixtures_raw(fixture_id, fx_full)
                    except Exception as raw_e:
                        print(f"    ! fixture {fixture_id}: match_fixtures_raw 저장 실패: {raw_e}", file=sys.stderr)

                    upsert_fixture_row(fx_full, int(lid), season)
                    upsert_match_row(fx_full, int(lid), season)

                    # ✅ 무거운 백필은 FINISHED만
                    if sg != "FINISHED":
                        continue

                    need_events = force or (not has_match_events(fixture_id))
                    need_lineups = force or (not has_lineups(fixture_id))
                    need_team_stats = force or (not has_team_stats(fixture_id))
                    need_player_stats = force or (not has_player_stats(fixture_id))

                    if not (need_events or need_lineups or need_team_stats or need_player_stats):
                        total_skipped += 1
                        continue

                    todo = []
                    if need_events:
                        todo.append("events")
                    if need_lineups:
                        todo.append("lineups")
                    if need_team_stats:
                        todo.append("team_stats")
                    if need_player_stats:
                        todo.append("player_stats")

                    print(f"    * fixture {fixture_id}: backfill={'+'.join(todo)}")
                    backfill_postmatch_for_fixture(
                        fixture_id,
                        do_events=need_events,
                        do_lineups=need_lineups,
                        do_team_stats=need_team_stats,
                        do_player_stats=need_player_stats,
                    )
                    total_new += 1

            except Exception as e:
                print(f"  ! season={target_season} league {lid} 처리 중 에러: {e}", file=sys.stderr)

        print(f"[postmatch_backfill] 완료. 신규={total_new}, 스킵={total_skipped}")
        return

    # ─────────────────────────────────────
    #  ✅ 날짜 모드(기존 로직 그대로)
    # ─────────────────────────────────────
    for target_date in target_dates:
        for lid in live_leagues:
            try:
                season_guess = pick_season_for_date(lid, target_date)


                # ✅ standings는 league+season 단위. season_guess가 틀릴 수 있어 fallback 시도
                if season_guess is not None:
                    # 후보 시즌: guess, guess+1, guess-1, 그리고 leagues API의 current 시즌(있으면)
                    seasons_to_try: List[int] = []

                    try:
                        seasons_to_try.append(int(season_guess))
                        seasons_to_try.append(int(season_guess) + 1)
                        seasons_to_try.append(int(season_guess) - 1)

                        # current 시즌 추가
                        seasons_meta = fetch_league_seasons(int(lid))
                        for sm in seasons_meta:
                            if sm.get("current") is True and sm.get("year") is not None:
                                try:
                                    seasons_to_try.append(int(sm["year"]))
                                except Exception:
                                    pass
                    except Exception:
                        pass

                    # 정리(중복 제거 + 비정상 연도 제거)
                    seasons_clean: List[int] = []
                    seen = set()
                    for y in seasons_to_try:
                        if not isinstance(y, int):
                            continue
                        if y < 1900 or y > 2100:
                            continue
                        if y in seen:
                            continue
                        seen.add(y)
                        seasons_clean.append(y)

                    inserted = False

                    for y in seasons_clean:
                        skey = (int(lid), int(y))

                        # 이미 이번 실행에서 성공적으로 넣은 시즌이면 스킵
                        if skey in fetched_standings_keys:
                            continue

                        need_st = force or (not has_standings(int(lid), int(y)))
                        if not need_st:
                            # 이미 DB에 있으면 이번엔 굳이 안 받음
                            fetched_standings_keys.add(skey)
                            continue

                        try:
                            st_rows = fetch_standings_from_api(int(lid), int(y))
                            if st_rows:
                                upsert_standings_rows(int(lid), int(y), st_rows)
                                fetched_standings_keys.add(skey)
                                print(f"    * standings league={lid} season={y}: rows={len(st_rows)}")
                                inserted = True
                                break
                            else:
                                # ✅ empty면 캐시로 막지 말고 다음 후보 시즌을 계속 시도
                                print(f"    ! standings league={lid} season={y}: empty response", file=sys.stderr)
                                continue
                        except Exception as se:
                            print(f"    ! standings league={lid} season={y} 에러: {se}", file=sys.stderr)
                            continue

                    # inserted=False면 정말로 API에 standings가 없거나(컵 등) 시즌이 더 다를 수 있음

                fixtures = fetch_fixtures_from_api(lid, target_date, season_guess)

                print(f"  - date={target_date} league {lid}: season={season_guess} fixtures={len(fixtures)}")


                for fx in fixtures:
                    basic = _extract_fixture_basic(fx)
                    if basic is None:
                        continue

                    fixture_id = basic["fixture_id"]
                    sg = (basic.get("status_group") or "").strip()

                    season = basic.get("season") or season_guess
                    if season is None:
                        continue

                    # ✅ 모든 상태(NS/INPLAY/FINISHED 포함)에서 fixtures/matches/raw는 항상 업서트
                    fx_full = fetch_fixture_by_id(fixture_id) or fx

                    try:
                        upsert_match_fixtures_raw(fixture_id, fx_full)
                    except Exception as raw_e:
                        print(f"    ! fixture {fixture_id}: match_fixtures_raw 저장 실패: {raw_e}", file=sys.stderr)

                    upsert_fixture_row(fx_full, lid, int(season))
                    upsert_match_row(fx_full, lid, int(season))

                    # ✅ 무거운 백필은 FINISHED만 (기존 정책 유지)
                    if sg != "FINISHED":
                        continue

                    need_events = force or (not has_match_events(fixture_id))
                    need_lineups = force or (not has_lineups(fixture_id))
                    need_team_stats = force or (not has_team_stats(fixture_id))
                    need_player_stats = force or (not has_player_stats(fixture_id))

                    if not (need_events or need_lineups or need_team_stats or need_player_stats):
                        total_skipped += 1
                        continue


                    todo = []
                    if need_events:
                        todo.append("events")
                    if need_lineups:
                        todo.append("lineups")
                    if need_team_stats:
                        todo.append("team_stats")
                    if need_player_stats:
                        todo.append("player_stats")

                    print(f"    * fixture {fixture_id}: backfill={'+'.join(todo)}")
                    backfill_postmatch_for_fixture(
                        fixture_id,
                        do_events=need_events,
                        do_lineups=need_lineups,
                        do_team_stats=need_team_stats,
                        do_player_stats=need_player_stats,
                    )
                    total_new += 1

            except Exception as e:
                print(f"  ! date={target_date} league {lid} 처리 중 에러: {e}", file=sys.stderr)

    print(f"[postmatch_backfill] 완료. 신규={total_new}, 스킵={total_skipped}")



if __name__ == "__main__":
    main()

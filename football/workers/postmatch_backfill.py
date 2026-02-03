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

# ✅ live_status_worker와 이벤트 레이스 정책을 공유하기 위한 role 설정
# - backfill/watchdog/postmatch 경로에서 INPLAY 이벤트를 건드리지 않게 보호 로직이 동작한다.
os.environ.setdefault("LIVE_WORKER_ROLE", "backfill")

# ✅ leagues(seasons) 캐시: league_id -> seasons list
_LEAGUE_SEASONS_CACHE: Dict[int, List[Dict[str, Any]]] = {}

# ─────────────────────────────────────
#  ✅ Teams / Leagues meta backfill (ADD ONLY)
# ─────────────────────────────────────

def _chunked(xs: List[int], size: int) -> List[List[int]]:
    out: List[List[int]] = []
    cur: List[int] = []
    for x in xs:
        cur.append(int(x))
        if len(cur) >= size:
            out.append(cur)
            cur = []
    if cur:
        out.append(cur)
    return out


def _missing_team_ids(team_ids: List[int]) -> List[int]:
    if not team_ids:
        return []
    rows = fetch_all(
        "SELECT id FROM teams WHERE id = ANY(%s)",
        (team_ids,),
    )
    existing = {int(r["id"]) for r in (rows or []) if r and r.get("id") is not None}
    return [tid for tid in team_ids if int(tid) not in existing]


def _missing_league_ids_or_logo(league_ids: List[int]) -> List[int]:
    """
    leagues row가 없거나, logo가 비어있는 league_id만 리턴
    """
    if not league_ids:
        return []
    rows = fetch_all(
        "SELECT id, logo FROM leagues WHERE id = ANY(%s)",
        (league_ids,),
    )
    have = {}
    for r in (rows or []):
        if not r or r.get("id") is None:
            continue
        have[int(r["id"])] = (r.get("logo") or "").strip()

    out: List[int] = []
    for lid in league_ids:
        logo = have.get(int(lid))
        if logo is None or logo == "":
            out.append(int(lid))
    return out


def _upsert_team_from_api(team_obj: Dict[str, Any]) -> None:
    """
    API /teams response element: {"team": {...}, "venue": {...}} 형태
    teams 테이블 스키마: id, name, country, logo
    """
    t = team_obj.get("team") or {}
    tid = t.get("id")
    if tid is None:
        return
    name = t.get("name")
    country = t.get("country")
    logo = t.get("logo")

    execute(
        """
        INSERT INTO teams (id, name, country, logo)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            name = COALESCE(NULLIF(EXCLUDED.name,''), teams.name),
            country = COALESCE(NULLIF(EXCLUDED.country,''), teams.country),
            logo = COALESCE(NULLIF(EXCLUDED.logo,''), teams.logo)
        """,
        (int(tid), name, country, logo),
    )


def _upsert_league_from_api(league_resp0: Dict[str, Any]) -> None:
    """
    API /leagues?id=xxx response[0] 형태:
      { "league": {...}, "country": {...}, "seasons": [...] }
    leagues 테이블 스키마: id, name, country, logo
    """
    lg = league_resp0.get("league") or {}
    lid = lg.get("id")
    if lid is None:
        return

    name = lg.get("name")
    logo = lg.get("logo")

    c = league_resp0.get("country") or {}
    country_name = c.get("name") or None

    execute(
        """
        INSERT INTO leagues (id, name, country, logo)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            name = COALESCE(NULLIF(EXCLUDED.name,''), leagues.name),
            country = COALESCE(NULLIF(EXCLUDED.country,''), leagues.country),
            logo = COALESCE(NULLIF(EXCLUDED.logo,''), leagues.logo)
        """,
        (int(lid), name, country_name, logo),
    )


def backfill_teams_meta(team_ids: List[int]) -> None:
    """
    team_ids 중 teams 테이블에 없는 것만 /teams?id= 로 채운다.
    """
    team_ids = sorted({int(x) for x in (team_ids or []) if x is not None})
    missing = _missing_team_ids(team_ids)
    if not missing:
        return

    print(f"[meta] missing teams: {len(missing)}")

    # API-Sports는 id 파라미터에 여러 개를 콤마로 받는 경우가 많음.
    # 안전하게 20개씩 쪼개 호출.
    for batch in _chunked(missing, 20):
        try:
            data = _safe_get("/teams", params={"id": ",".join(map(str, batch))})
            resp = data.get("response") or []
            for r in resp:
                if isinstance(r, dict):
                    _upsert_team_from_api(r)
        except Exception as e:
            print(f"[meta] teams batch failed ids={batch}: {e}", file=sys.stderr)


def backfill_leagues_meta(league_ids: List[int]) -> None:
    """
    league_ids 중 leagues 테이블 row가 없거나 logo가 빈 것만 /leagues?id= 로 채운다.
    """
    league_ids = sorted({int(x) for x in (league_ids or []) if x is not None})
    missing = _missing_league_ids_or_logo(league_ids)
    if not missing:
        return

    print(f"[meta] missing leagues/logo: {len(missing)}")

    # leagues는 보통 id 단건 호출이 확실해서 1개씩
    for lid in missing:
        try:
            data = _safe_get("/leagues", params={"id": int(lid)})
            resp = data.get("response") or []
            if resp and isinstance(resp, list) and isinstance(resp[0], dict):
                _upsert_league_from_api(resp[0])
        except Exception as e:
            print(f"[meta] leagues fetch failed id={lid}: {e}", file=sys.stderr)




# ─────────────────────────────────────
#  ENV / 유틸
# ─────────────────────────────────────

def _get_api_key() -> str:
    key = (
        os.environ.get("APIFOOTBALL_KEY")
        or os.environ.get("API_FOOTBALL_KEY")
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

    정책:
    - 시즌 모드에서도 UPCOMING(NS/TBD)까지 fixtures/matches/raw는 채워서
      "다음 해 일정(예: 2026년 경기)"이 DB에 존재하게 한다.
    - 무거운 백필(events/lineups/stats/players)은 기존대로 FINISHED에서만 수행(main 루프의 sg 체크로 유지)

    구현:
    - FINISHED_STATUSES(기본 FT,AET,PEN) + UPCOMING_STATUSES(기본 NS,TBD)를 합쳐서 요청/merge
    - API가 status 콤마를 허용하면 1콜 시도, 실패 시 status 분해 호출 후 merge
    """
    tz = (os.environ.get("API_TZ") or "Asia/Seoul").strip()

    finished_statuses = (os.environ.get("FINISHED_STATUSES") or "FT,AET,PEN").strip()
    upcoming_statuses = (os.environ.get("UPCOMING_STATUSES") or "NS,TBD").strip()

    statuses: List[str] = []
    for raw in (finished_statuses + "," + upcoming_statuses).split(","):
        s = raw.strip().upper()
        if s:
            statuses.append(s)

    # 중복 제거(순서 유지)
    seen = set()
    statuses = [s for s in statuses if not (s in seen or seen.add(s))]

    if not statuses:
        statuses = ["FT", "AET", "PEN", "NS", "TBD"]

    base_params: Dict[str, Any] = {
        "league": int(league_id),
        "season": int(season),
        "timezone": tz,
    }

    merged: Dict[int, Dict[str, Any]] = {}

    def _merge_rows(rows: Any) -> None:
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            basic = _extract_fixture_basic(r)
            if not basic:
                continue
            merged[basic["fixture_id"]] = r

    # 1) 콤마로 1번에 시도
    try:
        params = dict(base_params)
        params["status"] = ",".join(statuses)
        data = _safe_get("/fixtures", params=params)
        _merge_rows(data.get("response", []))
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
            _merge_rows(data.get("response", []))
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
            has_err = False
            if isinstance(errs, dict) and errs:
                has_err = True
            elif isinstance(errs, list) and len(errs) > 0:
                has_err = True

            if has_err:
                raise RuntimeError(f"API errors on {path} params={params}: {errs}")

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

def safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None

def safe_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        return str(x)
    except Exception:
        return None


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
    lid = int(league_id)

    cached = _LEAGUE_SEASONS_CACHE.get(lid)
    if isinstance(cached, list) and cached:
        return cached

    data = _safe_get("/leagues", params={"id": lid})
    resp = data.get("response") or []
    if not resp or not isinstance(resp, list) or not isinstance(resp[0], dict):
        _LEAGUE_SEASONS_CACHE[lid] = []
        return []

    seasons = (resp[0].get("seasons") or [])
    out = [s for s in seasons if isinstance(s, dict)]

    _LEAGUE_SEASONS_CACHE[lid] = out
    return out



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

def _get_table_columns(table_name: str) -> List[str]:
    """
    match_events / match_events_raw 컬럼이 환경마다 조금 다를 수 있어
    존재하는 컬럼만 사용하도록 1회 조회 후 캐시.
    (live_status_worker 최신 정책과 동일)
    """
    t = (table_name or "").strip().lower()
    if not t:
        return []

    if not hasattr(_get_table_columns, "_cache"):
        _get_table_columns._cache = {}  # type: ignore[attr-defined]
    cache: Dict[str, List[str]] = _get_table_columns._cache  # type: ignore[attr-defined]

    if t in cache:
        return cache[t]

    rows = fetch_all(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (t,),
    )
    cols: List[str] = []
    for r in rows or []:
        c = r.get("column_name")
        if isinstance(c, str) and c:
            cols.append(c.lower())
    cache[t] = cols
    return cols


# ─────────────────────────────────────
#  DB upserts (스키마 그대로)
# ─────────────────────────────────────

def upsert_match_fixtures_raw(fixture_id: int, fixture_obj: Dict[str, Any], fetched_at: dt.datetime) -> None:
    raw = json.dumps(fixture_obj, ensure_ascii=False, separators=(",", ":"))
    execute(
        """
        INSERT INTO match_fixtures_raw (fixture_id, data_json, fetched_at, updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            data_json   = EXCLUDED.data_json,
            fetched_at  = EXCLUDED.fetched_at,
            updated_at  = EXCLUDED.updated_at
        WHERE
            match_fixtures_raw.data_json IS DISTINCT FROM EXCLUDED.data_json
        """,
        (fixture_id, raw, fetched_at, fetched_at),
    )

def ensure_ft_triggers_table() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS ft_triggers (
            fixture_id               integer PRIMARY KEY,
            league_id                integer NOT NULL,
            season                   integer NOT NULL,
            finished_utc             text,
            standings_consumed_utc   text,
            bracket_consumed_utc     text,
            created_utc              text,
            updated_utc              text
        )
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_ft_triggers_created_utc ON ft_triggers (created_utc)")
    execute("CREATE INDEX IF NOT EXISTS idx_ft_triggers_league_season ON ft_triggers (league_id, season)")

def _iso_utc_now() -> str:
    return now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")

def enqueue_ft_trigger(fixture_id: int, league_id: int, season: int, finished_iso_utc: Optional[str] = None) -> None:
    fin = finished_iso_utc or _iso_utc_now()
    nowi = _iso_utc_now()
    execute(
        """
        INSERT INTO ft_triggers (
            fixture_id, league_id, season,
            finished_utc,
            standings_consumed_utc, bracket_consumed_utc,
            created_utc, updated_utc
        )
        VALUES (%s,%s,%s,%s,NULL,NULL,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            updated_utc  = EXCLUDED.updated_utc
        """,
        (int(fixture_id), int(league_id), int(season), fin, nowi, nowi),
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


def upsert_match_events_raw(fixture_id: int, events: List[Dict[str, Any]], fetched_at: dt.datetime) -> None:
    """
    match_events_raw에 원본 배열 저장(스키마 차이를 흡수).
    - data_json/raw_json/data 중 존재하는 컬럼에 저장
    - fetched_at/fetched_utc, updated_at/updated_utc 존재 시 함께 저장
    """
    cols = set(_get_table_columns("match_events_raw"))

    raw = json.dumps(events or [], ensure_ascii=False, separators=(",", ":"))

    col_data = "data_json" if "data_json" in cols else ("raw_json" if "raw_json" in cols else ("data" if "data" in cols else None))
    col_fetched = "fetched_at" if "fetched_at" in cols else ("fetched_utc" if "fetched_utc" in cols else None)
    col_updated = "updated_at" if "updated_at" in cols else ("updated_utc" if "updated_utc" in cols else None)

    if not col_data:
        return

    nowu = fetched_at.astimezone(dt.timezone.utc)
    ts_val = nowu.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    insert_cols = ["fixture_id", col_data]
    insert_vals: List[Any] = [fixture_id, raw]

    if col_fetched:
        insert_cols.append(col_fetched)
        insert_vals.append(ts_val)
    if col_updated:
        insert_cols.append(col_updated)
        insert_vals.append(ts_val)

    col_sql = ", ".join(insert_cols)
    ph_sql = ", ".join(["%s"] * len(insert_cols))

    if col_updated:
        upd_set = f"{col_data} = EXCLUDED.{col_data}, {col_updated} = EXCLUDED.{col_updated}"
        where_clause = f"match_events_raw.{col_data} IS DISTINCT FROM EXCLUDED.{col_data}"
    else:
        upd_set = f"{col_data} = EXCLUDED.{col_data}"
        where_clause = f"match_events_raw.{col_data} IS DISTINCT FROM EXCLUDED.{col_data}"

    execute(
        f"""
        INSERT INTO match_events_raw ({col_sql})
        VALUES ({ph_sql})
        ON CONFLICT (fixture_id) DO UPDATE SET
            {upd_set}
        WHERE
            {where_clause}
        """,
        tuple(insert_vals),
    )

def _table_exists(table_name: str) -> bool:
    # information_schema 조회 결과가 비어있으면 테이블이 없다고 간주
    cols = _get_table_columns(table_name)
    return bool(cols)

def upsert_generic_raw_snapshot(table_name: str, fixture_id: int, payload: Any, fetched_at: dt.datetime) -> None:
    """
    *스키마 변경 없이* raw 스냅샷 저장:
    - table_name이 없으면 조용히 return
    - data 컬럼 후보: data_json / raw_json / data
    - 시간 컬럼 후보: fetched_at|fetched_utc, updated_at|updated_utc
    """
    if not _table_exists(table_name):
        return

    cols = set(_get_table_columns(table_name))

    col_data = "data_json" if "data_json" in cols else ("raw_json" if "raw_json" in cols else ("data" if "data" in cols else None))
    col_fetched = "fetched_at" if "fetched_at" in cols else ("fetched_utc" if "fetched_utc" in cols else None)
    col_updated = "updated_at" if "updated_at" in cols else ("updated_utc" if "updated_utc" in cols else None)

    if not col_data:
        return

    raw = json.dumps(payload if payload is not None else [], ensure_ascii=False, separators=(",", ":"))

    nowu = fetched_at.astimezone(dt.timezone.utc)
    ts_val = nowu.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    insert_cols = ["fixture_id", col_data]
    insert_vals: List[Any] = [int(fixture_id), raw]

    if col_fetched:
        insert_cols.append(col_fetched)
        insert_vals.append(ts_val)
    if col_updated:
        insert_cols.append(col_updated)
        insert_vals.append(ts_val)

    col_sql = ", ".join(insert_cols)
    ph_sql = ", ".join(["%s"] * len(insert_cols))

    if col_updated:
        upd_set = f"{col_data} = EXCLUDED.{col_data}, {col_updated} = EXCLUDED.{col_updated}"
        where_clause = f"{table_name}.{col_data} IS DISTINCT FROM EXCLUDED.{col_data}"
    else:
        upd_set = f"{col_data} = EXCLUDED.{col_data}"
        where_clause = f"{table_name}.{col_data} IS DISTINCT FROM EXCLUDED.{col_data}"

    execute(
        f"""
        INSERT INTO {table_name} ({col_sql})
        VALUES ({ph_sql})
        ON CONFLICT (fixture_id) DO UPDATE SET
            {upd_set}
        WHERE
            {where_clause}
        """,
        tuple(insert_vals),
    )


def replace_match_events_for_fixture(fixture_id: int, events: List[Dict[str, Any]]) -> int:
    """
    match_events를 fixture_id 단위로 '싹 교체'한다. (API 스냅샷 미러링)

    정책(live 최신):
    - events가 빈 배열([])이면 DB를 건드리지 않는다(삭제/삽입 모두 안 함).
    - role != live 이고 DB matches.status_group 이 INPLAY면 레이스 차단을 위해 스킵한다.
    """
    cols = set(_get_table_columns("match_events"))
    if not cols:
        return 0

    if not events:
        return 0

    # ✅ 레이스 차단: backfill/watchdog/postmatch 경로에서 INPLAY 이벤트를 건드리지 않는다.
    try:
        role = (os.environ.get("LIVE_WORKER_ROLE") or "live").strip().lower()
        rows = fetch_all(
            """
            SELECT status_group
            FROM matches
            WHERE fixture_id = %s
            LIMIT 1
            """,
            (fixture_id,),
        )
        if rows:
            sg = (rows[0].get("status_group") or "").strip().upper()
            if (role != "live") and (sg == "INPLAY"):
                return 0
    except Exception:
        pass

    def has(c: str) -> bool:
        return c.lower() in cols

    col_extra = "extra" if has("extra") else ("time_extra" if has("time_extra") else None)

    inserted = 0

    # ✅ events가 있을 때만 삭제 후 교체
    execute("DELETE FROM match_events WHERE fixture_id = %s", (fixture_id,))

    for ev in events:
        if not isinstance(ev, dict):
            continue

        time_obj = ev.get("time") or {}
        team_obj = ev.get("team") or {}
        player_obj = ev.get("player") or {}
        assist_obj = ev.get("assist") or {}

        minute = safe_int(time_obj.get("elapsed")) or 0
        extra = safe_int(time_obj.get("extra"))

        type_raw = safe_text(ev.get("type"))
        detail_raw = safe_text(ev.get("detail"))
        comments_raw = safe_text(ev.get("comments"))

        team_id = safe_int(team_obj.get("id"))
        player_id = safe_int(player_obj.get("id"))
        player_name = safe_text(player_obj.get("name"))

        assist_id = safe_int(assist_obj.get("id"))
        assist_name = safe_text(assist_obj.get("name"))

        # SUB: player=OUT, assist=IN 이 흔함
        player_in_id = assist_id
        player_in_name = assist_name

        ins_cols: List[str] = []
        ins_vals: List[Any] = []

        def add(c: str, v: Any) -> None:
            if has(c):
                ins_cols.append(c)
                ins_vals.append(v)

        add("fixture_id", fixture_id)
        add("minute", minute)

        if col_extra:
            ins_cols.append(col_extra)
            ins_vals.append(extra)

        add("type", type_raw)
        add("detail", detail_raw)
        add("comments", comments_raw)

        add("team_id", team_id)

        add("player_id", player_id)
        add("player_name", player_name)

        add("assist_player_id", assist_id)
        add("assist_name", assist_name)

        add("player_in_id", player_in_id)
        add("player_in_name", player_in_name)

        if not ins_cols:
            continue

        col_sql = ", ".join(ins_cols)
        ph_sql = ", ".join(["%s"] * len(ins_cols))

        execute(
            f"INSERT INTO match_events ({col_sql}) VALUES ({ph_sql})",
            tuple(ins_vals),
        )
        inserted += 1

    return inserted


def upsert_match_events(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    raise RuntimeError(
        "DEPRECATED: use replace_match_events_for_fixture() + upsert_match_events_raw() "
        "(live 정책 동일화)"
    )


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
    updated_utc = now_utc().replace(microsecond=0).isoformat()

    for row in lineups:
        if not isinstance(row, dict):
            continue
        team_id = (row.get("team") or {}).get("id")
        if team_id is None:
            continue

        data_json = json.dumps(row, ensure_ascii=False, separators=(",", ":"))

        execute(
            """
            INSERT INTO match_lineups (fixture_id, team_id, data_json, updated_utc)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                data_json = EXCLUDED.data_json,
                updated_utc = EXCLUDED.updated_utc
            WHERE
                match_lineups.data_json IS DISTINCT FROM EXCLUDED.data_json
            """,
            (int(fixture_id), int(team_id), data_json, updated_utc),
        )


def upsert_match_team_stats(fixture_id: int, stats: List[Dict[str, Any]]) -> None:
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
                WHERE
                    match_team_stats.value IS DISTINCT FROM EXCLUDED.value
                """,
                (int(fixture_id), int(team_id), str(name), value_str),
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
                WHERE
                    match_player_stats.data_json IS DISTINCT FROM EXCLUDED.data_json
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

def has_match_events_raw(fixture_id: int) -> bool:
    return _has_any_row("match_events_raw", fixture_id)

def _has_any_row_if_table_exists(table: str, fixture_id: int) -> bool:
    # 테이블이 없으면 "이미 있다고 치고" 백필 필요 조건에서 제외
    if not _table_exists(table):
        return True
    return _has_any_row(table, fixture_id)

def has_lineups_raw(fixture_id: int) -> bool:
    return _has_any_row_if_table_exists("match_lineups_raw", fixture_id)

def has_team_stats_raw(fixture_id: int) -> bool:
    return _has_any_row_if_table_exists("match_team_stats_raw", fixture_id)

def has_player_stats_raw(fixture_id: int) -> bool:
    return _has_any_row_if_table_exists("match_player_stats_raw", fixture_id)




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
    do_events_raw: bool,
    do_lineups: bool,
    do_lineups_raw: bool,
    do_team_stats: bool,
    do_team_stats_raw: bool,
    do_player_stats: bool,
    do_player_stats_raw: bool,
) -> None:
    fetched_at = now_utc()

    # events / events_raw
    if do_events or do_events_raw:
        try:
            events = fetch_events_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: events 호출 에러: {e}", file=sys.stderr)
            events = []

        if do_events_raw:
            try:
                upsert_match_events_raw(fixture_id, events, fetched_at)
            except Exception:
                pass

        if do_events:
            try:
                replace_match_events_for_fixture(fixture_id, events)
            except Exception:
                pass

    # lineups / lineups_raw
    if do_lineups or do_lineups_raw:
        try:
            lineups = fetch_lineups_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: lineups 호출 에러: {e}", file=sys.stderr)
            lineups = []

        if do_lineups_raw:
            try:
                upsert_generic_raw_snapshot("match_lineups_raw", fixture_id, lineups, fetched_at)
            except Exception:
                pass

        if do_lineups and lineups:
            upsert_match_lineups(fixture_id, lineups)

    # team stats / team_stats_raw
    if do_team_stats or do_team_stats_raw:
        try:
            stats = fetch_team_stats_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: statistics 호출 에러: {e}", file=sys.stderr)
            stats = []

        if do_team_stats_raw:
            try:
                upsert_generic_raw_snapshot("match_team_stats_raw", fixture_id, stats, fetched_at)
            except Exception:
                pass

        if do_team_stats and stats:
            upsert_match_team_stats(fixture_id, stats)

    # player stats / player_stats_raw
    if do_player_stats or do_player_stats_raw:
        try:
            players_stats = fetch_player_stats_from_api(fixture_id)
        except Exception as e:
            print(f"    ! fixture {fixture_id}: players 호출 에러: {e}", file=sys.stderr)
            players_stats = []

        if do_player_stats_raw:
            try:
                upsert_generic_raw_snapshot("match_player_stats_raw", fixture_id, players_stats, fetched_at)
            except Exception:
                pass

        if do_player_stats and players_stats:
            upsert_match_player_stats(fixture_id, players_stats)





# ─────────────────────────────────────
#  엔트리
# ─────────────────────────────────────

def main() -> None:
    # ✅ live 워커 구조와 동일하게 FT 트리거 테이블 보장
    try:
        ensure_ft_triggers_table()
    except Exception:
        pass

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

    # ✅ 이번 실행에서 만난 팀/리그 id 수집 → 마지막에 meta backfill
    seen_team_ids: set[int] = set()
    seen_league_ids: set[int] = set()


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

                    # ✅ meta 수집
                    if basic.get("league_id") is not None:
                        seen_league_ids.add(int(basic["league_id"]))
                    if basic.get("home_id") is not None:
                        seen_team_ids.add(int(basic["home_id"]))
                    if basic.get("away_id") is not None:
                        seen_team_ids.add(int(basic["away_id"]))


                    # ✅ 시즌 모드에서는 season을 target_season으로 고정
                    season = int(target_season)

                    # ✅ 모든 상태에서 fixtures/matches/raw 업서트 (기존 정책 유지)
                    fx_full = fetch_fixture_by_id(fixture_id) or fx

                    try:
                        upsert_match_fixtures_raw(fixture_id, fx_full, now_utc())
                    except Exception as raw_e:
                        print(f"    ! fixture {fixture_id}: match_fixtures_raw 저장 실패: {raw_e}", file=sys.stderr)


                    upsert_fixture_row(fx_full, int(lid), season)
                    upsert_match_row(fx_full, int(lid), season)

                    # ✅ 무거운 백필은 FINISHED만
                    if sg != "FINISHED":
                        continue

                    # ✅ 브라켓/스탠딩 워커 동일화용: FT 트리거 기록
                    try:
                        enqueue_ft_trigger(fixture_id, int(lid), int(season))
                    except Exception:
                        pass

                    need_events = force or (not has_match_events(fixture_id))
                    need_events_raw = force or (not has_match_events_raw(fixture_id))
                    need_lineups = force or (not has_lineups(fixture_id))
                    need_team_stats = force or (not has_team_stats(fixture_id))
                    need_player_stats = force or (not has_player_stats(fixture_id))
                    need_lineups_raw = force or (not has_lineups_raw(fixture_id))
                    need_team_stats_raw = force or (not has_team_stats_raw(fixture_id))
                    need_player_stats_raw = force or (not has_player_stats_raw(fixture_id))

                    if not (
                        need_events or need_events_raw
                        or need_lineups or need_lineups_raw
                        or need_team_stats or need_team_stats_raw
                        or need_player_stats or need_player_stats_raw
                    ):
                        total_skipped += 1
                        continue



                    todo = []
                    if need_events:
                        todo.append("events")
                    elif need_events_raw:
                        todo.append("events_raw")

                    if need_lineups:
                        todo.append("lineups")
                    elif need_lineups_raw:
                        todo.append("lineups_raw")

                    if need_team_stats:
                        todo.append("team_stats")
                    elif need_team_stats_raw:
                        todo.append("team_stats_raw")

                    if need_player_stats:
                        todo.append("player_stats")
                    elif need_player_stats_raw:
                        todo.append("player_stats_raw")


                    print(f"    * fixture {fixture_id}: backfill={'+'.join(todo)}")
                    backfill_postmatch_for_fixture(
                        fixture_id,
                        do_events=need_events,
                        do_events_raw=need_events_raw,
                        do_lineups=need_lineups,
                        do_lineups_raw=need_lineups_raw,
                        do_team_stats=need_team_stats,
                        do_team_stats_raw=need_team_stats_raw,
                        do_player_stats=need_player_stats,
                        do_player_stats_raw=need_player_stats_raw,
                    )


                    total_new += 1

            except Exception as e:
                print(f"  ! season={target_season} league {lid} 처리 중 에러: {e}", file=sys.stderr)

        print(f"[postmatch_backfill] 완료. 신규={total_new}, 스킵={total_skipped}")
        # ✅ meta backfill (teams/leagues)
        try:
            if seen_league_ids:
                backfill_leagues_meta(sorted(seen_league_ids))
            if seen_team_ids:
                backfill_teams_meta(sorted(seen_team_ids))
        except Exception as me:
            print(f"[meta] backfill failed: {me}", file=sys.stderr)

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

                    # ✅ meta 수집
                    if basic.get("league_id") is not None:
                        seen_league_ids.add(int(basic["league_id"]))
                    if basic.get("home_id") is not None:
                        seen_team_ids.add(int(basic["home_id"]))
                    if basic.get("away_id") is not None:
                        seen_team_ids.add(int(basic["away_id"]))


                    season = basic.get("season") or season_guess
                    if season is None:
                        continue

                    # ✅ 모든 상태(NS/INPLAY/FINISHED 포함)에서 fixtures/matches/raw는 항상 업서트
                    fx_full = fetch_fixture_by_id(fixture_id) or fx

                    try:
                        upsert_match_fixtures_raw(fixture_id, fx_full, now_utc())
                    except Exception as raw_e:
                        print(f"    ! fixture {fixture_id}: match_fixtures_raw 저장 실패: {raw_e}", file=sys.stderr)


                    upsert_fixture_row(fx_full, lid, int(season))
                    upsert_match_row(fx_full, lid, int(season))

                    # ✅ 무거운 백필은 FINISHED만 (기존 정책 유지)
                    if sg != "FINISHED":
                        continue

                    # ✅ 브라켓/스탠딩 워커 동일화용: FT 트리거 기록
                    try:
                        enqueue_ft_trigger(fixture_id, int(lid), int(season))
                    except Exception:
                        pass

                    need_events = force or (not has_match_events(fixture_id))
                    need_events_raw = force or (not has_match_events_raw(fixture_id))
                    need_lineups = force or (not has_lineups(fixture_id))
                    need_team_stats = force or (not has_team_stats(fixture_id))
                    need_player_stats = force or (not has_player_stats(fixture_id))
                    need_lineups_raw = force or (not has_lineups_raw(fixture_id))
                    need_team_stats_raw = force or (not has_team_stats_raw(fixture_id))
                    need_player_stats_raw = force or (not has_player_stats_raw(fixture_id))

                    if not (
                        need_events or need_events_raw
                        or need_lineups or need_lineups_raw
                        or need_team_stats or need_team_stats_raw
                        or need_player_stats or need_player_stats_raw
                    ):
                        total_skipped += 1
                        continue




                    todo = []
                    if need_events:
                        todo.append("events")
                    elif need_events_raw:
                        todo.append("events_raw")

                    if need_lineups:
                        todo.append("lineups")
                    elif need_lineups_raw:
                        todo.append("lineups_raw")

                    if need_team_stats:
                        todo.append("team_stats")
                    elif need_team_stats_raw:
                        todo.append("team_stats_raw")

                    if need_player_stats:
                        todo.append("player_stats")
                    elif need_player_stats_raw:
                        todo.append("player_stats_raw")


                    print(f"    * fixture {fixture_id}: backfill={'+'.join(todo)}")
                    backfill_postmatch_for_fixture(
                        fixture_id,
                        do_events=need_events,
                        do_events_raw=need_events_raw,
                        do_lineups=need_lineups,
                        do_lineups_raw=need_lineups_raw,
                        do_team_stats=need_team_stats,
                        do_team_stats_raw=need_team_stats_raw,
                        do_player_stats=need_player_stats,
                        do_player_stats_raw=need_player_stats_raw,
                    )


                    total_new += 1

            except Exception as e:
                print(f"  ! date={target_date} league {lid} 처리 중 에러: {e}", file=sys.stderr)

    # ✅ meta backfill (teams/leagues)
    try:
        if seen_league_ids:
            backfill_leagues_meta(sorted(seen_league_ids))
        if seen_team_ids:
            backfill_teams_meta(sorted(seen_team_ids))
    except Exception as me:
        print(f"[meta] backfill failed: {me}", file=sys.stderr)


    print(f"[postmatch_backfill] 완료. 신규={total_new}, 스킵={total_skipped}")



if __name__ == "__main__":
    main()

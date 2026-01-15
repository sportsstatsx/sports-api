# live_status_worker.py (single-file live worker)
#
# 목표:
# - 이 파일 1개만으로 라이브 업데이트가 돌아가게 단순화
# - DB 스키마 변경 없음 (테이블/컬럼/PK 그대로 사용)
# - /fixtures 기반 상태/스코어 업데이트 + 원본 raw 저장(match_fixtures_raw)
# - INPLAY 경기: /events 저장 + events 기반 스코어 "정교 보정"(취소골/실축PK 제외, OG 반영)
# - INPLAY 경기: /statistics 60초 쿨다운
# - lineups: 프리매치(-60/-10 슬롯 1회씩) + 킥오프 직후(elapsed<=5) 재시도 정책
#
# 사용 테이블/PK (확인 완료):
# - fixtures(fixture_id PK)
# - matches(fixture_id PK)
# - match_fixtures_raw(fixture_id PK)
# - match_events(id PK)
# - match_events_raw(fixture_id PK)
# - match_lineups(fixture_id, team_id PK)
# - match_team_stats(fixture_id, team_id, name PK)
# - match_player_stats는 라이브에서 미사용(스키마 유지)

import os
import sys
import time
import json
import traceback
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import execute, fetch_all  # execute + schema 조회용 fetch_all 사용



# ─────────────────────────────────────
# ENV / 상수
# ─────────────────────────────────────

API_KEY = os.environ.get("APIFOOTBALL_KEY") or os.environ.get("API_FOOTBALL_KEY")
LIVE_LEAGUES_ENV = os.environ.get("LIVE_LEAGUES", "")
INTERVAL_SEC = int(os.environ.get("LIVE_WORKER_INTERVAL_SEC", "10"))

BASE = "https://v3.football.api-sports.io"
UA = "SportsStatsX-LiveWorker/1.0"

STATS_INTERVAL_SEC = 60  # stats 쿨다운
REQ_TIMEOUT = 12
REQ_RETRIES = 2


# ─────────────────────────────────────
# 런타임 캐시
# ─────────────────────────────────────

LAST_STATS_SYNC: Dict[int, float] = {}  # fixture_id -> last ts
LINEUPS_STATE: Dict[int, Dict[str, Any]] = {}  # fixture_id -> {"slot60":bool,"slot10":bool,"success":bool}


# ─────────────────────────────────────
# 유틸
# ─────────────────────────────────────

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(dtobj: dt.datetime) -> str:
    x = dtobj.astimezone(dt.timezone.utc)
    return x.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_live_leagues(env: str) -> List[int]:
    env = (env or "").strip()
    if not env:
        return []
    out: List[int] = []
    for part in env.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    # 중복 제거(순서 유지)
    seen = set()
    uniq: List[int] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def target_dates_for_live() -> List[str]:
    """
    기본은 UTC 오늘.
    (새벽 시간대 경기 누락 방지를 위해 필요하면 UTC 어제도 같이 조회)
    """
    now = now_utc()
    today = now.date()
    dates = [today.isoformat()]

    # UTC 00~03시는 어제 경기(자정 넘어가는 경기)가 INPLAY/FT로 남아있을 가능성이 높음
    if now.hour <= 3:
        dates.insert(0, (today - dt.timedelta(days=1)).isoformat())
    return dates


def map_status_group(short_code: Optional[str]) -> str:
    code = (short_code or "").upper().strip()

    # UPCOMING
    if code in ("NS", "TBD"):
        return "UPCOMING"

    # INPLAY (HT 포함)
    if code in ("1H", "2H", "ET", "P", "BT", "INT", "LIVE", "HT"):
        return "INPLAY"

    # FINISHED
    if code in ("FT", "AET", "PEN"):
        return "FINISHED"

    # 기타
    if code in ("SUSP", "PST", "CANC", "ABD", "AWD", "WO"):
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
        s = str(x)
        return s
    except Exception:
        return None


# ─────────────────────────────────────
# HTTP (API-Sports)
# ─────────────────────────────────────

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "x-apisports-key": API_KEY or "",
            "Accept": "application/json",
            "User-Agent": UA,
        }
    )
    return s


def api_get(session: requests.Session, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    API-Sports GET 호출 공통 함수.

    개선점(스키마 변경 없음):
    - ENV 기반 레이트리밋(RATE_LIMIT_PER_MIN / RATE_LIMIT_BURST) 적용 (토큰버킷)
    - 429(Too Many Requests) 대응: Retry-After 헤더 존중
    - 기존 재시도(REQ_RETRIES) 유지
    """
    url = f"{BASE}{path}"

    # --- rate limiter (token bucket) ---
    if not hasattr(api_get, "_rl"):
        # ENV가 없으면 기본값(기존 동작과 유사하게 너무 느리게 막지 않도록 넉넉히)
        try:
            per_min = float(os.environ.get("RATE_LIMIT_PER_MIN", "0") or "0")
        except Exception:
            per_min = 0.0
        try:
            burst = float(os.environ.get("RATE_LIMIT_BURST", "0") or "0")
        except Exception:
            burst = 0.0

        # 값이 0이면 '제한 없음'으로 취급(기존 동작 유지)
        rate_per_sec = (per_min / 60.0) if per_min > 0 else 0.0
        max_tokens = burst if burst > 0 else (max(1.0, rate_per_sec * 5) if rate_per_sec > 0 else 0.0)

        api_get._rl = {
            "rate": rate_per_sec,
            "max": max_tokens,
            "tokens": max_tokens,
            "ts": time.time(),
        }

    rl = api_get._rl  # type: ignore[attr-defined]

    def _acquire_token() -> None:
        rate = float(rl.get("rate") or 0.0)
        max_t = float(rl.get("max") or 0.0)
        if rate <= 0 or max_t <= 0:
            return  # 제한 없음

        now_ts = time.time()
        last_ts = float(rl.get("ts") or now_ts)
        elapsed = max(0.0, now_ts - last_ts)
        # refill
        tokens = float(rl.get("tokens") or 0.0) + elapsed * rate
        if tokens > max_t:
            tokens = max_t
        rl["tokens"] = tokens
        rl["ts"] = now_ts

        if tokens >= 1.0:
            rl["tokens"] = tokens - 1.0
            return

        # 부족하면 필요한 시간만큼 sleep
        need = 1.0 - tokens
        wait_sec = need / rate if rate > 0 else 0.25
        if wait_sec > 0:
            time.sleep(wait_sec)

        # 재획득(한 번 더 refill 후 1개 사용)
        now_ts2 = time.time()
        elapsed2 = max(0.0, now_ts2 - float(rl.get("ts") or now_ts2))
        tokens2 = float(rl.get("tokens") or 0.0) + elapsed2 * rate
        if tokens2 > max_t:
            tokens2 = max_t
        rl["tokens"] = max(0.0, tokens2 - 1.0)
        rl["ts"] = now_ts2

    last_err: Optional[Exception] = None
    for _ in range(REQ_RETRIES + 1):
        try:
            _acquire_token()
            r = session.get(url, params=params, timeout=REQ_TIMEOUT)

            # 429 대응
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                try:
                    wait = int(retry_after) if retry_after else 1
                except Exception:
                    wait = 1
                time.sleep(max(1, min(wait, 60)))
                raise requests.HTTPError("429 Too Many Requests", response=r)

            r.raise_for_status()
            data = r.json()
            return data
        except Exception as e:
            last_err = e
            time.sleep(0.4)

    raise last_err  # type: ignore



def fetch_fixtures(session: requests.Session, league_id: int, date_str: str, season: int) -> List[Dict[str, Any]]:
    data = api_get(session, "/fixtures", {"league": league_id, "date": date_str, "season": season})
    return (data.get("response") or []) if isinstance(data, dict) else []


def fetch_events(session: requests.Session, fixture_id: int) -> List[Dict[str, Any]]:
    data = api_get(session, "/fixtures/events", {"fixture": fixture_id})
    return (data.get("response") or []) if isinstance(data, dict) else []


def fetch_team_stats(session: requests.Session, fixture_id: int) -> List[Dict[str, Any]]:
    data = api_get(session, "/fixtures/statistics", {"fixture": fixture_id})
    return (data.get("response") or []) if isinstance(data, dict) else []


def fetch_lineups(session: requests.Session, fixture_id: int) -> List[Dict[str, Any]]:
    data = api_get(session, "/fixtures/lineups", {"fixture": fixture_id})
    return (data.get("response") or []) if isinstance(data, dict) else []


def infer_season_candidates(date_str: str) -> List[int]:
    """
    DB 의 season 테이블 등에 의존하지 않고도 안정적으로 시즌을 추론.
    - 먼저 date 연도
    - 그 다음 date 연도-1
    - 마지막으로 date 연도+1 (드물지만 컵/특수 케이스)
    """
    y = int(date_str[:4])
    return [y, y - 1, y + 1]


def _get_table_columns(table_name: str) -> List[str]:
    """
    public.<table_name> 컬럼 목록을 런타임에 조회해서 캐시한다.
    - 스키마 변경 없이, '있는 컬럼만' 동적으로 업서트하기 위한 기반.
    """
    if not hasattr(_get_table_columns, "_cache"):
        _get_table_columns._cache = {}  # type: ignore[attr-defined]

    cache: Dict[str, List[str]] = _get_table_columns._cache  # type: ignore[attr-defined]
    if table_name in cache:
        return cache[table_name]

    rows = fetch_all(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )

    cols: List[str] = []
    # db 래퍼가 dict/list/tuple 어떤 형태로 주든 최대한 흡수
    for r in rows or []:
        if isinstance(r, dict):
            v = r.get("column_name")
        elif isinstance(r, (list, tuple)) and len(r) >= 1:
            v = r[0]
        else:
            v = None
        if v:
            cols.append(str(v))

    cache[table_name] = cols
    return cols



# ─────────────────────────────────────
# DB Upsert
# ─────────────────────────────────────

def upsert_fixture_row(
    fixture_id: int,
    league_id: Optional[int],
    season: Optional[int],
    date_utc: Optional[str],
    status_short: Optional[str],
    status_group: Optional[str],
) -> None:
    # 변경이 있을 때만 UPDATE (DB write/bloat 감소)
    execute(
        """
        INSERT INTO fixtures (fixture_id, league_id, season, date_utc, status, status_group)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id     = EXCLUDED.league_id,
            season        = EXCLUDED.season,
            date_utc      = EXCLUDED.date_utc,
            status        = EXCLUDED.status,
            status_group  = EXCLUDED.status_group
        WHERE
            fixtures.league_id    IS DISTINCT FROM EXCLUDED.league_id OR
            fixtures.season       IS DISTINCT FROM EXCLUDED.season OR
            fixtures.date_utc     IS DISTINCT FROM EXCLUDED.date_utc OR
            fixtures.status       IS DISTINCT FROM EXCLUDED.status OR
            fixtures.status_group IS DISTINCT FROM EXCLUDED.status_group
        """,
        (fixture_id, league_id, season, date_utc, status_short, status_group),
    )



def upsert_match_row_from_fixture(
    fixture_obj: Dict[str, Any],
    league_id: Optional[int],
    season: Optional[int],
) -> Tuple[int, int, int, str, str]:
    """
    fixtures 응답 1개(item)를 matches 테이블로 업서트.
    반환: (fixture_id, home_id, away_id, status_group, date_utc)

    핵심 변경:
    - matches 테이블 컬럼 목록을 런타임에 조회해서(_get_table_columns),
      "DB에 실제로 존재하는 컬럼만" 대상으로 INSERT/UPDATE SQL을 동적으로 생성한다.
    - /fixtures 응답에서 만들 수 있는 값들은 최대한 많이 준비해두고,
      그 중 matches에 존재하는 컬럼만 골라서 채운다.
    - 스키마에 없는 컬럼을 건드려서 워커가 죽는 문제를 원천 차단.
    """

    fx = fixture_obj.get("fixture") or {}
    teams = fixture_obj.get("teams") or {}
    goals = fixture_obj.get("goals") or {}
    score = fixture_obj.get("score") or {}
    league = fixture_obj.get("league") or {}

    fixture_id = safe_int(fx.get("id"))
    if fixture_id is None:
        raise ValueError("fixture_id missing")

    date_utc = safe_text(fx.get("date")) or ""

    st = fx.get("status") or {}
    status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
    status_elapsed = safe_int(st.get("elapsed"))

    status_group = map_status_group(status_short)
    status = status_short.strip() or "UNK"  # (matches.status가 NOT NULL인 경우 대비)

    home = (teams.get("home") or {}) if isinstance(teams, dict) else {}
    away = (teams.get("away") or {}) if isinstance(teams, dict) else {}
    home_id = safe_int(home.get("id")) or 0
    away_id = safe_int(away.get("id")) or 0

    home_ft = safe_int(goals.get("home")) if isinstance(goals, dict) else None
    away_ft = safe_int(goals.get("away")) if isinstance(goals, dict) else None

    ht = (score.get("halftime") or {}) if isinstance(score, dict) else {}
    ft = (score.get("fulltime") or {}) if isinstance(score, dict) else {}
    et = (score.get("extratime") or {}) if isinstance(score, dict) else {}
    pn = (score.get("penalty") or {}) if isinstance(score, dict) else {}

    home_ht = safe_int(ht.get("home"))
    away_ht = safe_int(ht.get("away"))

    # 참고: 스키마에 있을 수도 있는 추가 스코어들(있으면 채움)
    home_ft_score = safe_int(ft.get("home"))
    away_ft_score = safe_int(ft.get("away"))
    home_et = safe_int(et.get("home"))
    away_et = safe_int(et.get("away"))
    home_pen = safe_int(pn.get("home"))
    away_pen = safe_int(pn.get("away"))

    # venue/referee (있으면 채움)
    venue = fx.get("venue") or {}
    referee = safe_text(fx.get("referee"))

    venue_id = safe_int(venue.get("id")) if isinstance(venue, dict) else None
    venue_name = safe_text(venue.get("name")) if isinstance(venue, dict) else None
    venue_city = safe_text(venue.get("city")) if isinstance(venue, dict) else None

    # team names/logos/winner (스키마에 있을 때만 들어가도록 후보 준비)
    home_name = safe_text(home.get("name"))
    away_name = safe_text(away.get("name"))
    home_logo = safe_text(home.get("logo"))
    away_logo = safe_text(away.get("logo"))
    home_winner = home.get("winner") if isinstance(home, dict) else None
    away_winner = away.get("winner") if isinstance(away, dict) else None

    # league fields (스키마에 있을 때만)
    league_name = safe_text(league.get("name")) if isinstance(league, dict) else None
    league_country = safe_text(league.get("country")) if isinstance(league, dict) else None
    league_round = safe_text(league.get("round")) if isinstance(league, dict) else None

    # ---- DB 컬럼에 맞춰 "후보 값"을 넉넉히 준비 ----
    # (여기 키들이 matches에 실제로 존재하면 자동으로 INSERT/UPDATE에 포함됨)
    candidates: Dict[str, Any] = {
        # 거의 확실히 있을 핵심
        "fixture_id": fixture_id,
        "league_id": league_id,
        "season": season,
        "date_utc": date_utc,
        "status": status,
        "status_group": status_group,
        "home_id": home_id,
        "away_id": away_id,
        "home_ft": home_ft,
        "away_ft": away_ft,
        "elapsed": status_elapsed,
        "home_ht": home_ht,
        "away_ht": away_ht,

        # 스키마에 있을 가능성이 높은 확장(있으면 자동 채움)
        "referee": referee,
        "venue_id": venue_id,
        "venue_name": venue_name,
        "venue_city": venue_city,

        "home_name": home_name,
        "away_name": away_name,
        "home_logo": home_logo,
        "away_logo": away_logo,
        "home_winner": home_winner,
        "away_winner": away_winner,

        "league_name": league_name,
        "league_country": league_country,
        "league_round": league_round,

        # 다양한 스키마 케이스 대응 (컬럼명이 이렇게 존재하면 채워짐)
        "home_fulltime": home_ft_score,
        "away_fulltime": away_ft_score,
        "home_et": home_et,
        "away_et": away_et,
        "home_pen": home_pen,
        "away_pen": away_pen,

        # 혹시 score_* 형태 스키마인 경우
        "home_ft_score": home_ft_score,
        "away_ft_score": away_ft_score,
        "home_ht_score": home_ht,
        "away_ht_score": away_ht,
        "home_et_score": home_et,
        "away_et_score": away_et,
        "home_pen_score": home_pen,
        "away_pen_score": away_pen,
    }

    # ---- matches 실제 컬럼과 교집합만 사용 ----
    cols = _get_table_columns("matches")
    colset = set(cols)

    # fixture_id는 반드시 포함(충돌키/PK)
    if "fixture_id" not in colset:
        raise RuntimeError("matches table has no fixture_id column")

    # 실제로 INSERT/UPDATE에 쓸 컬럼만 추림(컬럼 존재 + 후보 키 존재)
    use_cols: List[str] = []
    for k in cols:
        if k in candidates:
            use_cols.append(k)

    # 안전: fixture_id는 무조건 포함되게
    if "fixture_id" not in use_cols:
        use_cols.insert(0, "fixture_id")

    # INSERT 컬럼/값
    insert_cols_sql = ",\n            ".join(use_cols)
    insert_vals_sql = ",".join(["%s"] * len(use_cols))
    insert_params = tuple(candidates.get(c) for c in use_cols)

    # UPDATE 컬럼(=fixture_id 제외)
    upd_cols = [c for c in use_cols if c != "fixture_id"]

    # 업데이트할 게 없으면(극단 케이스) insert만 시도
    if not upd_cols:
        execute(
            f"""
            INSERT INTO matches (
                {insert_cols_sql}
            )
            VALUES (
                {insert_vals_sql}
            )
            ON CONFLICT (fixture_id) DO NOTHING
            """,
            insert_params,
        )
        return fixture_id, home_id, away_id, status_group, date_utc

    update_set_sql = ",\n            ".join([f"{c} = EXCLUDED.{c}" for c in upd_cols])
    where_diff_sql = " OR\n            ".join([f"matches.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in upd_cols])

    execute(
        f"""
        INSERT INTO matches (
            {insert_cols_sql}
        )
        VALUES (
            {insert_vals_sql}
        )
        ON CONFLICT (fixture_id) DO UPDATE SET
            {update_set_sql}
        WHERE
            {where_diff_sql}
        """,
        insert_params,
    )

    return fixture_id, home_id, away_id, status_group, date_utc








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



def upsert_match_events_raw(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    raw = json.dumps(events, ensure_ascii=False, separators=(",", ":"))
    execute(
        """
        INSERT INTO match_events_raw (fixture_id, data_json)
        VALUES (%s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            data_json = EXCLUDED.data_json
        WHERE
            match_events_raw.data_json IS DISTINCT FROM EXCLUDED.data_json
        """,
        (fixture_id, raw),
    )



def upsert_match_events(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    """
    match_events 스키마(현재 dev):
      id(bigint PK), fixture_id, team_id, player_id, type, detail, minute(not null),
      extra(default 0), assist_player_id, assist_name, player_in_id, player_in_name

    - 공급자가 동일 이벤트를 다른 id로 재발급하는 케이스 대비: in-memory signature dedupe
    - signature 안정화: ev_type/detail normalize(대소문/공백/구두점 차이 완화)
    """

    def _norm(s: Optional[str]) -> str:
        if not s:
            return ""
        x = str(s).lower().strip()
        x = " ".join(x.split())
        for ch in ("'", '"', "`", ".", ",", ":", ";", "!", "?", "(", ")", "[", "]", "{", "}", "|"):
            x = x.replace(ch, "")
        return x

    # fixture 단위 signature cache: {fixture_id: {sig_tuple: last_seen_ts}}
    if not hasattr(upsert_match_events, "_sig_cache"):
        upsert_match_events._sig_cache = {}  # type: ignore[attr-defined]
    sig_cache: Dict[int, Dict[Tuple[Any, ...], float]] = upsert_match_events._sig_cache  # type: ignore[attr-defined]

    now_ts = time.time()
    seen = sig_cache.get(fixture_id)
    if seen is None:
        seen = {}
        sig_cache[fixture_id] = seen

    # 오래된 signature 정리
    if (len(seen) > 800) or (now_ts - min(seen.values(), default=now_ts) > 1800):
        cutoff = now_ts - 1800
        for k, v in list(seen.items()):
            if v < cutoff:
                del seen[k]
        if len(seen) > 1200:
            for k, _ in sorted(seen.items(), key=lambda kv: kv[1])[: len(seen) - 800]:
                del seen[k]

    for ev in events or []:
        ev_id = safe_int(ev.get("id"))
        if ev_id is None:
            continue

        team = ev.get("team") or {}
        player = ev.get("player") or {}
        assist = ev.get("assist") or {}

        t_id = safe_int(team.get("id"))
        p_id = safe_int(player.get("id"))
        a_id = safe_int(assist.get("id"))

        ev_type = safe_text(ev.get("type"))
        detail = safe_text(ev.get("detail"))

        tm = ev.get("time") or {}
        minute = safe_int(tm.get("elapsed"))
        extra = safe_int(tm.get("extra"))

        # minute NOT NULL → 없으면 스킵(스키마 위반 방지)
        if minute is None:
            continue

        # signature dedupe (id가 바뀌어도 동일 이벤트면 스킵)
        sig = (minute, extra, _norm(ev_type), _norm(detail), t_id, p_id, a_id)
        prev_ts = seen.get(sig)
        if prev_ts is not None and (now_ts - prev_ts) < 600:
            continue
        seen[sig] = now_ts

        # substitution 관련 player_in은 현재 스키마에 있으니 자리만 유지(나중에 파싱 가능)
        player_in_id = None
        player_in_name = None

        execute(
            """
            INSERT INTO match_events (
                id,
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
            VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                ev_id,
                fixture_id,
                t_id,
                p_id,
                ev_type,
                detail,
                minute,
                extra,
                a_id,
                safe_text(assist.get("name")),
                player_in_id,
                player_in_name,
            ),
        )







def upsert_match_team_stats(fixture_id: int, stats_resp: List[Dict[str, Any]]) -> None:
    """
    /fixtures/statistics response:
    [
      { team: {id,name}, statistics: [{type,value}, ...] },
      ...
    ]
    """
    for team_block in stats_resp or []:
        team = team_block.get("team") or {}
        team_id = safe_int(team.get("id"))
        if team_id is None:
            continue

        stats = team_block.get("statistics") or []
        for s in stats:
            name = safe_text(s.get("type"))
            if not name:
                continue
            val = s.get("value")
            # value는 숫자/문자/퍼센트/None 등 다양 → text로 저장
            value_txt = None if val is None else str(val)

            execute(
                """
                INSERT INTO match_team_stats (fixture_id, team_id, name, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fixture_id, team_id, name) DO UPDATE SET
                    value = EXCLUDED.value
                WHERE
                    match_team_stats.value IS DISTINCT FROM EXCLUDED.value
                """,
                (fixture_id, team_id, name, value_txt),
            )



def upsert_match_lineups(fixture_id: int, lineups_resp: List[Dict[str, Any]], updated_at: dt.datetime) -> bool:
    """
    match_lineups PK: (fixture_id, team_id)
    응답이 유의미하면 True 반환.
    """
    if not lineups_resp:
        return False

    ok_any = False
    updated_utc = iso_utc(updated_at)

    for item in lineups_resp:
        team = item.get("team") or {}
        team_id = safe_int(team.get("id"))
        if team_id is None:
            continue

        raw = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        execute(
            """
            INSERT INTO match_lineups (fixture_id, team_id, data_json, updated_utc)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                data_json   = EXCLUDED.data_json,
                updated_utc = EXCLUDED.updated_utc
            WHERE
                match_lineups.data_json IS DISTINCT FROM EXCLUDED.data_json
            """,
            (fixture_id, team_id, raw, updated_utc),
        )
        ok_any = True

    return ok_any



# ─────────────────────────────────────
# 이벤트 기반 스코어 보정 (정교화 핵심)
# ─────────────────────────────────────

def calc_score_from_events(
    events: List[Dict[str, Any]],
    home_id: int,
    away_id: int,
) -> Tuple[int, int]:
    """
    Goal + Var 이벤트를 함께 사용해서 "최종 득점"을 계산한다.

    ✅ Goal 이벤트는 일단 득점 후보로 쌓는다.
    ✅ Var 이벤트 중
       - Goal Disallowed / Goal cancelled / No Goal  => 직전 Goal 1개를 취소 처리
       - Goal confirmed                              => 유지(아무것도 안 함)
    ✅ Own Goal은 반대팀 득점으로 처리 (기존 유지)
    ✅ Missed Penalty(실축)는 득점에서 제외 (기존 유지)

    주의:
    - API-Sports 데이터에서 '취소/무효'는 Goal.detail이 아니라 Var.type으로 내려오는 케이스가 많음
    """

    def _norm(s: Optional[str]) -> str:
        if not s:
            return ""
        x = str(s).lower().strip()
        x = " ".join(x.split())
        return x

    def _time_key(ev: Dict[str, Any], fallback_idx: int) -> Tuple[int, int, int]:
        tm = ev.get("time") or {}
        el = safe_int(tm.get("elapsed"))
        ex = safe_int(tm.get("extra"))
        # elapsed/extra가 None이면 뒤로 밀리게 안전값
        elv = el if el is not None else 10**9
        exv = ex if ex is not None else 0
        return (elv, exv, fallback_idx)

    # Goal 이벤트에서 '무효/취소'로 볼만한 텍스트(Goal.detail에 실제로 붙는 경우만 처리)
    invalid_markers = (
        "cancel",        # cancelled
        "disallow",      # disallowed
        "no goal",       # no goal
        "offside",       # offside
        "foul",          # foul
        "annul",         # annulled(드물지만)
        "null",          # nullified(드물지만)
    )

    # 득점 후보 리스트(Var로 취소되면 cancelled=True로 마킹)
    # 각 항목: {
    #   "scoring_team_id": int,   # 실제 득점 팀(OG면 반대팀)
    #   "source_team_id": int,    # 이벤트 team_id (OG면 원래 자책팀)
    #   "elapsed": Optional[int],
    #   "extra": Optional[int],
    #   "cancelled": bool,
    # }
    goals: List[Dict[str, Any]] = []

    # 시간순 정렬 + 동시간대는 원본 순서(인덱스) 유지
    indexed = list(enumerate(events or []))
    indexed.sort(key=lambda pair: _time_key(pair[1], pair[0]))
    evs = [ev for _, ev in indexed]


    def _add_goal(ev: Dict[str, Any]) -> None:
        detail = _norm(ev.get("detail"))
        # 실축PK 제외
        if "missed penalty" in detail:
            return
        if ("miss" in detail) and ("pen" in detail):
            return

        # Goal.detail에 취소/무효 문구가 붙는(드문) 케이스 방어
        if any(m in detail for m in invalid_markers) and ("own goal" not in detail):
            return

        team = ev.get("team") or {}
        team_id = safe_int(team.get("id"))
        if team_id is None:
            return

        tm = ev.get("time") or {}
        elapsed = safe_int(tm.get("elapsed"))
        extra = safe_int(tm.get("extra"))

        is_og = "own goal" in detail

        # 득점 팀 결정
        scoring_team_id = team_id
        if is_og:
            if team_id == home_id:
                scoring_team_id = away_id
            elif team_id == away_id:
                scoring_team_id = home_id

        goals.append(
            {
                "scoring_team_id": scoring_team_id,
                "source_team_id": team_id,
                "elapsed": elapsed,
                "extra": extra,
                "cancelled": False,
            }
        )

    def _apply_var(ev: Dict[str, Any]) -> None:
        """
        Var 이벤트로 직전 Goal을 취소/유지 처리
        - Goal Disallowed / Goal cancelled / No Goal => 직전 Goal 1개 취소
        - Goal confirmed => 유지(아무것도 안 함)
        """
        detail = _norm(ev.get("detail"))
        if not detail:
            return

        is_disallow = ("goal disallowed" in detail) or ("goal cancelled" in detail) or ("no goal" in detail)
        is_confirm = "goal confirmed" in detail

        # 골 관련 var이 아니면 무시(패널티/레드카드 등)
        if not (is_disallow or is_confirm):
            return

        # confirmed면 굳이 할 작업 없음(이미 골로 카운트되었을 것)
        if is_confirm:
            return

        # 여기서부터는 disallow/cancel/no goal => 직전 Goal 1개 취소
        team = ev.get("team") or {}
        var_team_id = safe_int(team.get("id"))  # 있는 경우가 많음(네 출력에서도 team 정보가 보통 있음)
        tm = ev.get("time") or {}
        var_elapsed = safe_int(tm.get("elapsed"))
        var_extra = safe_int(tm.get("extra"))

        # 보수적 취소 규칙:
        # - var_elapsed 가 없으면 취소하지 않음(오탐 방지)
        # - 시간 매칭은 단계적으로: 같은 elapsed -> ±1 -> (마지막 수단) ±2
        # - 팀 정보(var_team_id)가 있으면 일치하는 goal을 우선 취소
        if var_elapsed is None:
            return

        def _pick_cancel_idx(max_delta: int) -> Optional[int]:
            best: Optional[int] = None
            for i in range(len(goals) - 1, -1, -1):
                g = goals[i]
                if g.get("cancelled"):
                    continue

                g_el = g.get("elapsed")
                if g_el is None:
                    continue  # 시간 없는 goal은 보수적으로 제외

                if abs(g_el - var_elapsed) > max_delta:
                    continue

                # 팀 매칭 우선
                if var_team_id is not None:
                    if (g.get("source_team_id") == var_team_id) or (g.get("scoring_team_id") == var_team_id):
                        return i
                    # 팀 불일치는 후보로만(동일 delta 내에서 fallback)
                    if best is None:
                        best = i
                else:
                    # 팀 정보가 없으면 시간만으로 가장 최근 것을 선택
                    return i

            return best

        best_idx = _pick_cancel_idx(0)   # 같은 elapsed
        if best_idx is None:
            best_idx = _pick_cancel_idx(1)   # ±1
        if best_idx is None:
            best_idx = _pick_cancel_idx(2)   # 마지막 수단 ±2

        if best_idx is not None:
            goals[best_idx]["cancelled"] = True


    # 메인 루프
    for ev in evs:
        ev_type = _norm(ev.get("type"))
        if ev_type == "goal":
            _add_goal(ev)
        elif ev_type == "var":
            _apply_var(ev)
        else:
            continue

    # 최종 합계
    h = 0
    a = 0
    for g in goals:
        if g.get("cancelled"):
            continue
        tid = g.get("scoring_team_id")
        if tid == home_id:
            h += 1
        elif tid == away_id:
            a += 1

    return h, a




def update_live_score_if_needed(fixture_id: int, status_group: str, home_goals: int, away_goals: int) -> None:
    """
    live 중에만 안전하게 덮어쓰기.
    - status_group 인자는 이미 run_once()에서 판단한 값이므로,
      DB의 status_group='INPLAY' 조건을 중복으로 걸지 않음(타이밍 이슈로 UPDATE 스킵 방지).
    - 값이 바뀔 때만 UPDATE 해서 불필요한 DB write를 줄임
    """
    if status_group != "INPLAY":
        return

    execute(
        """
        UPDATE matches
        SET home_ft = %s,
            away_ft = %s
        WHERE fixture_id = %s
          AND (
              matches.home_ft IS DISTINCT FROM %s OR
              matches.away_ft IS DISTINCT FROM %s
          )
        """,
        (home_goals, away_goals, fixture_id, home_goals, away_goals),
    )




# ─────────────────────────────────────
# 라인업 정책
# ─────────────────────────────────────

def _ensure_lineups_state(fixture_id: int) -> Dict[str, Any]:
    st = LINEUPS_STATE.get(fixture_id)
    if not st:
        st = {"slot60": False, "slot10": False, "success": False}
        LINEUPS_STATE[fixture_id] = st
    return st


def maybe_sync_lineups(
    session: requests.Session,
    fixture_id: int,
    date_utc: str,
    status_group: str,
    elapsed: Optional[int],
    now: dt.datetime,
) -> None:
    st = _ensure_lineups_state(fixture_id)
    if st.get("success"):
        return

    kickoff: Optional[dt.datetime] = None
    try:
        # date_utc 는 ISO8601 (예: 2026-01-15T12:00:00+00:00)
        kickoff = dt.datetime.fromisoformat(date_utc.replace("Z", "+00:00"))
        if kickoff.tzinfo is None:
            kickoff = kickoff.replace(tzinfo=dt.timezone.utc)
        else:
            kickoff = kickoff.astimezone(dt.timezone.utc)
    except Exception:
        kickoff = None

    nowu = now.astimezone(dt.timezone.utc)

    # 프리매치 슬롯(-60/-10): 응답이 비어도 "시도"는 마킹해서 반복 호출 방지
    if kickoff and status_group == "UPCOMING":
        mins = int((kickoff - nowu).total_seconds() / 60)

        # -60 슬롯: 59~61분 사이
        if (59 <= mins <= 61) and not st.get("slot60"):
            st["slot60"] = True
            try:
                resp = fetch_lineups(session, fixture_id)
                ok = upsert_match_lineups(fixture_id, resp, nowu)
                if ok:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} slot60 ok={ok}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} slot60 err: {e}", file=sys.stderr)
            return

        # -10 슬롯: 9~11분 사이
        if (9 <= mins <= 11) and not st.get("slot10"):
            st["slot10"] = True
            try:
                resp = fetch_lineups(session, fixture_id)
                ok = upsert_match_lineups(fixture_id, resp, nowu)
                if ok:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} slot10 ok={ok}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} slot10 err: {e}", file=sys.stderr)
            return

        return

    # 킥오프 직후(INPLAY) 재시도: elapsed<=5 동안은 빈 응답이면 계속 시도 가능
    if status_group == "INPLAY":
        el = elapsed if elapsed is not None else -1
        if 0 <= el <= 5:
            try:
                resp = fetch_lineups(session, fixture_id)
                ok = upsert_match_lineups(fixture_id, resp, nowu)
                if ok:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} inplay(el={el}) ok={ok}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} inplay err: {e}", file=sys.stderr)


# ─────────────────────────────────────
# 메인 1회 실행
# ─────────────────────────────────────

def run_once() -> None:
    if not API_KEY:
        print("[live_status_worker] APIFOOTBALL_KEY(env) 가 비어있습니다. 종료.", file=sys.stderr)
        return

    league_ids = parse_live_leagues(LIVE_LEAGUES_ENV)
    if not league_ids:
        print("[live_status_worker] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return

    dates = target_dates_for_live()
    now = now_utc()
    fetched_at = now

    s = _session()

    # ─────────────────────────────────────
    # (1) league/date 시즌 & 무경기 캐시 (API 낭비 감소)
    # ─────────────────────────────────────
    if not hasattr(run_once, "_fixtures_cache"):
        # key: (league_id, date_str) -> {"season": int|None, "no": bool, "exp": float}
        run_once._fixtures_cache = {}  # type: ignore[attr-defined]
    fc: Dict[Tuple[int, str], Dict[str, Any]] = run_once._fixtures_cache  # type: ignore[attr-defined]

    # TTL (초) - 스키마 변경 없이 호출만 줄임
    SEASON_TTL = 60 * 60      # 시즌 확정 캐시 60분
    NOFIX_TTL = 60 * 10       # 그 날짜 경기 없음 캐시 10분

    now_ts = time.time()
    # 만료 엔트리 정리
    for k, v in list(fc.items()):
        if float(v.get("exp") or 0) < now_ts:
            del fc[k]

    total_fixtures = 0
    total_inplay = 0

    # 이번 run에서 본 fixture들의 상태(캐시 prune에 사용)
    fixture_groups: Dict[int, str] = {}

    for date_str in dates:
        for lid in league_ids:
            fixtures: List[Dict[str, Any]] = []
            used_season: Optional[int] = None

            cache_key = (lid, date_str)
            cached = fc.get(cache_key)
            if cached and float(cached.get("exp") or 0) >= now_ts:
                if cached.get("no") is True:
                    # 최근에 '그 날짜 경기 없음'으로 판정된 리그/날짜는 잠시 스킵
                    continue
                cached_season = cached.get("season")
                if isinstance(cached_season, int):
                    try:
                        rows = fetch_fixtures(s, lid, date_str, cached_season)
                        if rows:
                            fixtures = rows
                            used_season = cached_season
                        else:
                            # 캐시 시즌에서 빈 결과면 캐시를 무효화하고 후보를 다시 탐색
                            fc.pop(cache_key, None)
                    except Exception as e:
                        # 캐시 시즌 호출 실패 시 후보 탐색으로 fallback
                        fc.pop(cache_key, None)
                        print(f"  [fixtures] league={lid} date={date_str} season={cached_season} err: {e}", file=sys.stderr)

            # 캐시 미스/무효일 때: 시즌 후보를 돌려서 "응답이 있는 시즌"을 선택
            if used_season is None:
                for season in infer_season_candidates(date_str):
                    try:
                        rows = fetch_fixtures(s, lid, date_str, season)
                        if rows:
                            fixtures = rows
                            used_season = season
                            # 시즌 캐시
                            fc[cache_key] = {"season": season, "no": False, "exp": now_ts + SEASON_TTL}
                            break
                    except Exception as e:
                        # 시즌 시도 중 오류는 다음 후보로
                        last = str(e)
                        print(f"  [fixtures] league={lid} date={date_str} season={season} err: {last}", file=sys.stderr)

            if used_season is None:
                # 결과가 없는 건 흔함(그 날짜에 경기 없음) → 짧게 캐시
                fc[cache_key] = {"season": None, "no": True, "exp": now_ts + NOFIX_TTL}
                continue

            total_fixtures += len(fixtures)
            print(f"[fixtures] league={lid} date={date_str} season={used_season} count={len(fixtures)}")

            for item in fixtures:
                try:
                    # matches / fixtures / raw upsert
                    fx = item.get("fixture") or {}
                    fid = safe_int(fx.get("id"))
                    if fid is None:
                        continue

                    st = fx.get("status") or {}
                    # (7) short/code 통일
                    status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
                    status_group = map_status_group(status_short)
                    fixture_groups[fid] = status_group

                    # fixtures 테이블(요약)
                    upsert_fixture_row(
                        fixture_id=fid,
                        league_id=lid,
                        season=used_season,
                        date_utc=safe_text(fx.get("date")),
                        status_short=status_short,
                        status_group=status_group,
                    )

                    # matches 테이블(상세)
                    fixture_id, home_id, away_id, sg, date_utc = upsert_match_row_from_fixture(
                        item, league_id=lid, season=used_season
                    )

                    # raw 저장(match_fixtures_raw)
                    try:
                        upsert_match_fixtures_raw(fixture_id, item, fetched_at)
                    except Exception as raw_err:
                        print(f"      [match_fixtures_raw] fixture_id={fixture_id} err: {raw_err}", file=sys.stderr)

                    # lineups 정책 적용(UPCOMING도 여기서)
                    try:
                        elapsed = safe_int((item.get("fixture") or {}).get("status", {}).get("elapsed"))
                        maybe_sync_lineups(s, fixture_id, date_utc, sg, elapsed, now)
                    except Exception as lu_err:
                        print(f"      [lineups] fixture_id={fixture_id} policy err: {lu_err}", file=sys.stderr)

                    # INPLAY 처리
                    if sg != "INPLAY":
                        continue

                    total_inplay += 1

                    # 1) events 저장 + 스코어 보정(단일 경로)
                    try:
                        events = fetch_events(s, fixture_id)
                        upsert_match_events_raw(fixture_id, events)
                        upsert_match_events(fixture_id, events)

                        # 이벤트 기반 스코어 계산(정교화)
                        h, a = calc_score_from_events(events, home_id, away_id)
                        update_live_score_if_needed(fixture_id, sg, h, a)

                        print(f"      [events] fixture_id={fixture_id} goals(events)={h}:{a} events={len(events)}")
                    except Exception as ev_err:
                        print(f"      [events] fixture_id={fixture_id} err: {ev_err}", file=sys.stderr)

                    # 2) stats (60초 쿨다운)
                    try:
                        now_ts2 = time.time()
                        last_ts = LAST_STATS_SYNC.get(fixture_id)
                        if (last_ts is None) or ((now_ts2 - last_ts) >= STATS_INTERVAL_SEC):
                            stats = fetch_team_stats(s, fixture_id)
                            upsert_match_team_stats(fixture_id, stats)
                            LAST_STATS_SYNC[fixture_id] = now_ts2
                            print(f"      [stats] fixture_id={fixture_id} updated")
                    except Exception as st_err:
                        print(f"      [stats] fixture_id={fixture_id} err: {st_err}", file=sys.stderr)

                except Exception as e:
                    print(f"  ! fixture 처리 중 에러: {e}", file=sys.stderr)

    # ─────────────────────────────────────
    # (6) 런타임 캐시 prune (메모리 누적 방지)
    # ─────────────────────────────────────
    try:
        # FINISHED/OTHER는 더 이상 필요 없으므로 캐시 제거
        for fid, g in list(fixture_groups.items()):
            if g in ("FINISHED", "OTHER"):
                LAST_STATS_SYNC.pop(fid, None)
                LINEUPS_STATE.pop(fid, None)
                # upsert_match_events signature cache 제거
                sig_cache = getattr(upsert_match_events, "_sig_cache", None)
                if isinstance(sig_cache, dict):
                    sig_cache.pop(fid, None)

        # 아주 오래된 LINEUPS_STATE도 정리(혹시 오늘/어제 범위를 벗어났을 때)
        if len(LINEUPS_STATE) > 3000:
            # 최근에 쓴다고 보장할 수 없으니, 과감히 일부만 남김
            for fid in list(LINEUPS_STATE.keys())[: len(LINEUPS_STATE) - 2000]:
                LINEUPS_STATE.pop(fid, None)
    except Exception:
        pass

    print(f"[live_status_worker] done. total_fixtures={total_fixtures}, inplay={total_inplay}")



# ─────────────────────────────────────
# 루프
# ─────────────────────────────────────

def loop() -> None:
    print(f"[live_status_worker] start (interval={INTERVAL_SEC}s)")
    while True:
        try:
            run_once()
        except Exception:
            traceback.print_exc()
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    loop()

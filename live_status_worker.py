# live_status_worker.py (single-file live worker)
#
# 목표:
# - 이 파일 1개만으로 라이브 업데이트가 돌아가게 단순화
# - DB 스키마 변경 없음 (테이블/컬럼/PK 그대로 사용)
# - /fixtures 기반 상태/스코어 업데이트 + 원본 raw 저장(match_fixtures_raw)
# - INPLAY 경기: /events 스냅샷 미러링 저장(match_events/match_events_raw) + red 요약(match_live_state)
# - INPLAY 경기: /statistics 60초 쿨다운
# - lineups: 프리매치(-60/-10 슬롯 1회씩) + INPLAY 초반(elapsed<=15, 20s 쿨다운) 재시도 정책
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

from db import execute, fetch_all  # dev 스키마 확정 → 런타임 schema 조회 불필요




# ─────────────────────────────────────
# ENV / 상수
# ─────────────────────────────────────

API_KEY = os.environ.get("APIFOOTBALL_KEY") or os.environ.get("API_FOOTBALL_KEY")
LIVE_LEAGUES_ENV = os.environ.get("LIVE_LEAGUES", "")

# ✅ (A) 라이브 감지 주기: live=all 호출 간격(초)
DETECT_INTERVAL_SEC = int(os.environ.get("LIVE_DETECT_INTERVAL_SEC", "10"))

# ✅ (B) 리그별 스캔 모드(/fixtures league/date 스캔) 기본 간격(초)
DEFAULT_SCAN_INTERVAL_SEC = int(os.environ.get("DEFAULT_LIVE_FIXTURES_INTERVAL_SEC", "10"))

# ✅ (B-2) 라이브가 0일 때(=watched_live=0)에도 "종료 반영/포스트매치"를 위해 느리게 스캔(초)
# 기본 90초(60~120 권장). 필요하면 ENV로 조절.
IDLE_SCAN_INTERVAL_SEC = int(os.environ.get("IDLE_LIVE_FIXTURES_INTERVAL_SEC", "90"))

# ✅ (C) “핵심 리그는 5초” 같은 오버라이드 목록
# 예) "39,140,135"  (여기에 포함된 리그만 5초)
FAST_LEAGUES_ENV = os.environ.get("FAST_LIVE_LEAGUES", "")


# (구버전 호환: 기존 INTERVAL_SEC는 더 이상 루프 sleep에 직접 쓰지 않음)
INTERVAL_SEC = int(os.environ.get("LIVE_WORKER_INTERVAL_SEC", str(DETECT_INTERVAL_SEC)))


BASE = "https://v3.football.api-sports.io"
UA = "SportsStatsX-LiveWorker/1.0"

STATS_INTERVAL_SEC = 60   # stats 쿨다운은 유지
REQ_TIMEOUT = 12
REQ_RETRIES = 2




# ─────────────────────────────────────
# 런타임 캐시
# ─────────────────────────────────────

LAST_STATS_SYNC: Dict[int, float] = {}   # fixture_id -> last ts
LINEUPS_STATE: Dict[int, Dict[str, Any]] = {}  # fixture_id -> {"slot60":bool,"slot10":bool,"success":bool}

# ✅ 리그별 스캔 모드에서 /fixtures(league/date) 호출 간격 제어
LAST_FIXTURES_SCAN_TS: Dict[Tuple[int, str], float] = {}

# ✅ 최근에 라이브였던 리그를 잠깐 더 "빠르게" 스캔해서 FT 반영 누락 방지
# - live=all 에서 리그가 빠지는 순간(막 종료)에도 /fixtures 스캔을 유지하기 위함
RECENT_LIVE_LEAGUE_TS: Dict[int, float] = {}
RECENT_LIVE_LEAGUE_TTL_SEC = int(os.environ.get("RECENT_LIVE_LEAGUE_TTL_SEC", "3600"))  # 기본 1시간






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

def parse_fast_leagues(env: str) -> List[int]:
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


def fetch_live_all(session: requests.Session) -> List[Dict[str, Any]]:
    """
    ✅ live=all 지원 확인 완료.
    - 전세계 라이브를 1콜로 조회
    """
    data = api_get(session, "/fixtures", {"live": "all"})
    return (data.get("response") or []) if isinstance(data, dict) else []



def target_dates_for_live(live_items: Optional[List[Dict[str, Any]]] = None) -> List[str]:
    """
    FT 누락 방지용 날짜 선택(호출 증가 최소화 버전)

    핵심:
    - 기본은 UTC "어제 + 오늘"만 스캔 (이 조합이 '90분쯤 응답에서 경기 사라짐'을 막는 핵심)
    - '내일'은 호출을 크게 늘릴 수 있어 제거
    - 대신 live=all(또는 watched_live)에서 실제로 라이브로 잡힌 경기들의 fixture.date(UTC)에서
      YYYY-MM-DD 를 추출해 그 날짜만 추가 스캔 (필요할 때만)
    """
    now = now_utc()
    today = now.date()

    dates: List[str] = [
        (today - dt.timedelta(days=1)).isoformat(),
        today.isoformat(),
    ]

    # live_items에서 kickoff UTC 날짜를 추출해서 추가(필요한 날짜만)
    if live_items:
        for it in live_items:
            fx = it.get("fixture") or {}
            d = safe_text(fx.get("date")) or ""
            # "2026-01-25T..." 형태에서 날짜만
            if len(d) >= 10 and d[4] == "-" and d[7] == "-":
                dates.append(d[:10])

    # 중복 제거(순서 유지)
    seen = set()
    uniq: List[str] = []
    for d in dates:
        if d in seen:
            continue
        seen.add(d)
        uniq.append(d)
    return uniq




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


def ensure_match_live_state_table() -> None:
    """
    레드카드 요약용(타임라인/이벤트로그와 분리)
    - fixture_id 당 1줄
    - /api/fixtures에서 LEFT JOIN 해서 즉시 표기 안정화 목적
    """
    execute(
        """
        CREATE TABLE IF NOT EXISTS match_live_state (
            fixture_id  integer PRIMARY KEY,
            home_red    integer,
            away_red    integer,
            updated_utc text
        )
        """
    )


def upsert_match_live_state(
    fixture_id: int,
    home_red: int,
    away_red: int,
    updated_at: dt.datetime,
) -> None:
    updated_utc = iso_utc(updated_at)
    execute(
        """
        INSERT INTO match_live_state (fixture_id, home_red, away_red, updated_utc)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            home_red    = EXCLUDED.home_red,
            away_red    = EXCLUDED.away_red,
            updated_utc = EXCLUDED.updated_utc
        WHERE
            match_live_state.home_red IS DISTINCT FROM EXCLUDED.home_red OR
            match_live_state.away_red IS DISTINCT FROM EXCLUDED.away_red OR
            match_live_state.updated_utc IS DISTINCT FROM EXCLUDED.updated_utc
        """,
        (fixture_id, int(home_red), int(away_red), updated_utc),
    )


def calc_red_cards_from_events(
    events: List[Dict[str, Any]],
    home_id: int,
    away_id: int,
) -> Tuple[int, int]:
    """
    ✅ 라이브 중에는 타임라인을 저장/표시하지 않고,
       /fixtures/events에서 레드카드 요약만 뽑아 즉시성 있게 유지한다.

    - 기본: detail == 'Red Card' 카운트
    - 안전장치: 공급자가 'Second Yellow card'만 주는 리그가 있을 수 있어 레드로 취급(원치 않으면 제거 가능)
    """

    def _norm(s: Any) -> str:
        if s is None:
            return ""
        try:
            x = str(s).strip().lower()
            x = " ".join(x.split())
            return x
        except Exception:
            return ""

    home_red = 0
    away_red = 0

    for ev in events or []:
        ev_type = _norm(ev.get("type"))
        if ev_type != "card":
            continue

        detail = _norm(ev.get("detail"))
        if detail not in ("red card", "second yellow card"):
            continue

        team = ev.get("team") or {}
        t_id = safe_int(team.get("id"))
        if t_id == home_id:
            home_red += 1
        elif t_id == away_id:
            away_red += 1

    return home_red, away_red

# ─────────────────────────────────────
# FT 이후 타임라인 채우기 (정책: FT 감지 후 +60초 1회, +30분 1회)
# - 기존 INPLAY 수집 방식은 건드리지 않음
# - FINISHED일 때만 별도 동작
# - 스키마 변경 없음(단, 상태 추적용 추가 테이블 1개 생성)
# ─────────────────────────────────────

def ensure_match_postmatch_timeline_state_table() -> None:
    """
    FT 최초 감지 시각과 2회 실행 여부를 저장.
    - fixture_id 당 1줄
    - 60초 후 1회, 30분 후 1회만 실행되게 제어
    """
    execute(
        """
        CREATE TABLE IF NOT EXISTS match_postmatch_timeline_state (
            fixture_id        integer PRIMARY KEY,
            ft_first_seen_utc  text,
            done_60           integer,
            done_30m          integer,
            updated_utc       text
        )
        """
    )


def _get_table_columns(table_name: str) -> List[str]:
    """
    match_events / match_events_raw 컬럼이 환경마다 조금 다를 수 있어
    존재하는 컬럼만 사용하도록 1회 조회 후 캐시.
    (다른 수집 로직은 절대 건드리지 않음)
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
    cols = []
    for r in rows or []:
        c = r.get("column_name")
        if isinstance(c, str) and c:
            cols.append(c.lower())
    cache[t] = cols
    return cols


def _read_postmatch_state(fixture_id: int) -> Dict[str, Any] | None:
    rows = fetch_all(
        """
        SELECT fixture_id, ft_first_seen_utc, done_60, done_30m, updated_utc
        FROM match_postmatch_timeline_state
        WHERE fixture_id = %s
        """,
        (fixture_id,),
    )
    return rows[0] if rows else None


def _init_postmatch_state_if_missing(fixture_id: int, now: dt.datetime) -> Dict[str, Any]:
    st = _read_postmatch_state(fixture_id)
    if st:
        return st

    nowu = now.astimezone(dt.timezone.utc)
    now_iso = iso_utc(nowu)

    execute(
        """
        INSERT INTO match_postmatch_timeline_state (fixture_id, ft_first_seen_utc, done_60, done_30m, updated_utc)
        VALUES (%s, %s, 0, 0, %s)
        ON CONFLICT (fixture_id) DO NOTHING
        """,
        (fixture_id, now_iso, now_iso),
    )
    return _read_postmatch_state(fixture_id) or {
        "fixture_id": fixture_id,
        "ft_first_seen_utc": now_iso,
        "done_60": 0,
        "done_30m": 0,
        "updated_utc": now_iso,
    }


def _mark_postmatch_done(fixture_id: int, which: str, now: dt.datetime) -> None:
    nowu = now.astimezone(dt.timezone.utc)
    now_iso = iso_utc(nowu)

    if which == "60":
        execute(
            """
            UPDATE match_postmatch_timeline_state
            SET done_60 = 1, updated_utc = %s
            WHERE fixture_id = %s
              AND (done_60 IS DISTINCT FROM 1)
            """,
            (now_iso, fixture_id),
        )
    elif which == "30m":
        execute(
            """
            UPDATE match_postmatch_timeline_state
            SET done_30m = 1, updated_utc = %s
            WHERE fixture_id = %s
              AND (done_30m IS DISTINCT FROM 1)
            """,
            (now_iso, fixture_id),
        )


def upsert_match_events_raw(fixture_id: int, events: List[Dict[str, Any]], fetched_at: dt.datetime) -> None:
    """
    match_events_raw에 원본 배열 저장(스키마 차이를 흡수).
    """
    cols = set(_get_table_columns("match_events_raw"))

    raw = json.dumps(events or [], ensure_ascii=False, separators=(",", ":"))

    # 가능한 컬럼명 후보
    col_data = "data_json" if "data_json" in cols else ("raw_json" if "raw_json" in cols else ("data" if "data" in cols else None))
    col_fetched = "fetched_at" if "fetched_at" in cols else ("fetched_utc" if "fetched_utc" in cols else None)
    col_updated = "updated_at" if "updated_at" in cols else ("updated_utc" if "updated_utc" in cols else None)

    if not col_data:
        # data 컬럼을 못 찾으면 raw 저장은 생략(타임라인 insert는 별도 진행)
        return

    nowu = fetched_at.astimezone(dt.timezone.utc)
    ts_val = iso_utc(nowu)

    # INSERT/UPDATE 구성
    insert_cols = ["fixture_id", col_data]
    insert_vals = [fixture_id, raw]

    if col_fetched:
        insert_cols.append(col_fetched)
        insert_vals.append(ts_val)
    if col_updated:
        insert_cols.append(col_updated)
        insert_vals.append(ts_val)

    col_sql = ", ".join(insert_cols)
    ph_sql = ", ".join(["%s"] * len(insert_cols))

    # 업데이트는 data_json만 변경 시 수행(기존 스타일 유지)
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


def replace_match_events_for_fixture(fixture_id: int, events: List[Dict[str, Any]]) -> int:
    """
    match_events를 fixture_id 단위로 '싹 교체'한다. (API 스냅샷 미러링)
    - 스키마에 존재하는 컬럼만 채움
    - ✅ events가 비어있어도 'DELETE 후 0건 INSERT' 그대로 반영 (API 흔들림도 그대로 따라감)
    반환: insert된 row 수
    """
    cols = set(_get_table_columns("match_events"))
    if not cols:
        return 0

    # 최소 필수(있을 때만 사용)
    def has(c: str) -> bool:
        return c.lower() in cols

    # 컬럼명 호환(둘 중 하나 존재)
    col_extra = "extra" if has("extra") else ("time_extra" if has("time_extra") else None)

    inserted = 0

    # ✅ 항상 기존 fixture 이벤트 삭제(스냅샷 교체)
    execute("DELETE FROM match_events WHERE fixture_id = %s", (fixture_id,))

    # events가 비어있으면 여기서 끝 (DB는 빈 상태로 유지)
    if not events:
        return 0

    for ev in (events or []):
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

        # SUB의 경우: API-Sports는 보통 player=OUT, assist=IN 으로 옴
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



def maybe_sync_postmatch_timeline(
    session: requests.Session,
    fixture_id: int,
    status_group: str,
    now: dt.datetime,
) -> None:
    """
    정책 고정:
    - status_group == FINISHED 일 때만 동작
    - FT 최초 감지 시각 기준
      * +60초 1회
      * +30분 1회
    """
    if status_group != "FINISHED":
        return

    st = _init_postmatch_state_if_missing(fixture_id, now)

    ft_seen = st.get("ft_first_seen_utc")
    try:
        base = dt.datetime.fromisoformat(str(ft_seen).replace("Z", "+00:00"))
        if base.tzinfo is None:
            base = base.replace(tzinfo=dt.timezone.utc)
        else:
            base = base.astimezone(dt.timezone.utc)
    except Exception:
        base = now.astimezone(dt.timezone.utc)

    nowu = now.astimezone(dt.timezone.utc)

    done_60 = int(st.get("done_60") or 0) == 1
    done_30m = int(st.get("done_30m") or 0) == 1

    # 1) +60초 1회
    if (not done_60) and (nowu >= (base + dt.timedelta(seconds=60))):
        events = fetch_events(session, fixture_id)
        try:
            upsert_match_events_raw(fixture_id, events, nowu)
        except Exception:
            pass

        ins = 0
        try:
            ins = replace_match_events_for_fixture(fixture_id, events)
        except Exception:
            ins = 0

        _mark_postmatch_done(fixture_id, "60", nowu)
        print(f"      [postmatch_timeline] fixture_id={fixture_id} +60s events={len(events)} inserted={ins}")

    # 2) +30분 1회
    if (not done_30m) and (nowu >= (base + dt.timedelta(minutes=30))):
        events = fetch_events(session, fixture_id)
        try:
            upsert_match_events_raw(fixture_id, events, nowu)
        except Exception:
            pass

        ins = 0
        try:
            ins = replace_match_events_for_fixture(fixture_id, events)
        except Exception:
            ins = 0

        _mark_postmatch_done(fixture_id, "30m", nowu)
        print(f"      [postmatch_timeline] fixture_id={fixture_id} +30m events={len(events)} inserted={ins}")




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
    dev 스키마(matches) 정확 매핑 업서트.
    반환: (fixture_id, home_id, away_id, status_group, date_utc)

    matches 컬럼(확인됨):
      fixture_id(PK), league_id, season, date_utc, status, status_group,
      home_id, away_id, home_ft, away_ft, elapsed, home_ht, away_ht,
      referee, fixture_timezone, fixture_timestamp,
      status_short, status_long, status_elapsed, status_extra,
      venue_id, venue_name, venue_city, league_round
    """

    # ---- 필수 입력(스키마 NOT NULL) ----
    if league_id is None:
        raise ValueError("league_id is required (matches.league_id NOT NULL)")
    if season is None:
        raise ValueError("season is required (matches.season NOT NULL)")

    fx = fixture_obj.get("fixture") or {}
    teams = fixture_obj.get("teams") or {}
    goals = fixture_obj.get("goals") or {}
    score = fixture_obj.get("score") or {}
    league = fixture_obj.get("league") or {}

    fixture_id = safe_int(fx.get("id"))
    if fixture_id is None:
        raise ValueError("fixture_id missing")

    date_utc = safe_text(fx.get("date")) or ""
    if not date_utc:
        raise ValueError("date_utc missing (matches.date_utc NOT NULL)")

    # ---- status ----
    st = fx.get("status") or {}
    status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
    status_long = safe_text(st.get("long")) or ""
    status_elapsed = safe_int(st.get("elapsed"))
    status_extra = safe_int(st.get("extra"))  # 없으면 None

    status_group = map_status_group(status_short)
    status = (status_short or "").strip() or "UNK"  # matches.status NOT NULL

    # ---- teams ----
    home = (teams.get("home") or {}) if isinstance(teams, dict) else {}
    away = (teams.get("away") or {}) if isinstance(teams, dict) else {}
    home_id = safe_int(home.get("id")) or 0
    away_id = safe_int(away.get("id")) or 0
    if home_id == 0 or away_id == 0:
        # matches.home_id/away_id NOT NULL
        raise ValueError("home_id/away_id missing (matches.home_id/away_id NOT NULL)")

    # ---- goals / halftime ----
    home_ft = safe_int(goals.get("home")) if isinstance(goals, dict) else None
    away_ft = safe_int(goals.get("away")) if isinstance(goals, dict) else None

    ht = (score.get("halftime") or {}) if isinstance(score, dict) else {}
    home_ht = safe_int(ht.get("home"))
    away_ht = safe_int(ht.get("away"))

    # elapsed 컬럼은 matches.elapsed (별도) → status_elapsed를 그대로 씀(네 스키마에 elapsed 존재)
    elapsed = status_elapsed

    # ---- fixture meta ----
    referee = safe_text(fx.get("referee"))
    fixture_timezone = safe_text(fx.get("timezone"))
    fixture_timestamp = None
    try:
        # API-Sports fixture.timestamp는 보통 int(유닉스)
        fixture_timestamp = safe_int(fx.get("timestamp"))
    except Exception:
        fixture_timestamp = None

    venue = fx.get("venue") or {}
    venue_id = safe_int(venue.get("id")) if isinstance(venue, dict) else None
    venue_name = safe_text(venue.get("name")) if isinstance(venue, dict) else None
    venue_city = safe_text(venue.get("city")) if isinstance(venue, dict) else None

    league_round = safe_text(league.get("round")) if isinstance(league, dict) else None

    execute(
        """
        INSERT INTO matches (
            fixture_id,
            league_id,
            season,
            date_utc,
            status,
            status_group,
            home_id,
            away_id,
            home_ft,
            away_ft,
            elapsed,
            home_ht,
            away_ht,
            referee,
            fixture_timezone,
            fixture_timestamp,
            status_short,
            status_long,
            status_elapsed,
            status_extra,
            venue_id,
            venue_name,
            venue_city,
            league_round
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id         = EXCLUDED.league_id,
            season            = EXCLUDED.season,
            date_utc          = EXCLUDED.date_utc,
            status            = EXCLUDED.status,
            status_group      = EXCLUDED.status_group,
            home_id           = EXCLUDED.home_id,
            away_id           = EXCLUDED.away_id,
            home_ft           = EXCLUDED.home_ft,
            away_ft           = EXCLUDED.away_ft,
            elapsed           = EXCLUDED.elapsed,
            home_ht           = EXCLUDED.home_ht,
            away_ht           = EXCLUDED.away_ht,
            referee           = EXCLUDED.referee,
            fixture_timezone  = EXCLUDED.fixture_timezone,
            fixture_timestamp = EXCLUDED.fixture_timestamp,
            status_short      = EXCLUDED.status_short,
            status_long       = EXCLUDED.status_long,
            status_elapsed    = EXCLUDED.status_elapsed,
            status_extra      = EXCLUDED.status_extra,
            venue_id          = EXCLUDED.venue_id,
            venue_name        = EXCLUDED.venue_name,
            venue_city        = EXCLUDED.venue_city,
            league_round      = EXCLUDED.league_round
        WHERE
            matches.league_id         IS DISTINCT FROM EXCLUDED.league_id OR
            matches.season            IS DISTINCT FROM EXCLUDED.season OR
            matches.date_utc          IS DISTINCT FROM EXCLUDED.date_utc OR
            matches.status            IS DISTINCT FROM EXCLUDED.status OR
            matches.status_group      IS DISTINCT FROM EXCLUDED.status_group OR
            matches.home_id           IS DISTINCT FROM EXCLUDED.home_id OR
            matches.away_id           IS DISTINCT FROM EXCLUDED.away_id OR
            matches.home_ft           IS DISTINCT FROM EXCLUDED.home_ft OR
            matches.away_ft           IS DISTINCT FROM EXCLUDED.away_ft OR
            matches.elapsed           IS DISTINCT FROM EXCLUDED.elapsed OR
            matches.home_ht           IS DISTINCT FROM EXCLUDED.home_ht OR
            matches.away_ht           IS DISTINCT FROM EXCLUDED.away_ht OR
            matches.referee           IS DISTINCT FROM EXCLUDED.referee OR
            matches.fixture_timezone  IS DISTINCT FROM EXCLUDED.fixture_timezone OR
            matches.fixture_timestamp IS DISTINCT FROM EXCLUDED.fixture_timestamp OR
            matches.status_short      IS DISTINCT FROM EXCLUDED.status_short OR
            matches.status_long       IS DISTINCT FROM EXCLUDED.status_long OR
            matches.status_elapsed    IS DISTINCT FROM EXCLUDED.status_elapsed OR
            matches.status_extra      IS DISTINCT FROM EXCLUDED.status_extra OR
            matches.venue_id          IS DISTINCT FROM EXCLUDED.venue_id OR
            matches.venue_name        IS DISTINCT FROM EXCLUDED.venue_name OR
            matches.venue_city        IS DISTINCT FROM EXCLUDED.venue_city OR
            matches.league_round      IS DISTINCT FROM EXCLUDED.league_round
        """,
        (
            fixture_id,
            league_id,
            season,
            date_utc,
            status,
            status_group,
            home_id,
            away_id,
            home_ft,
            away_ft,
            elapsed,
            home_ht,
            away_ht,
            referee,
            fixture_timezone,
            fixture_timestamp,
            status_short or None,
            status_long or None,
            status_elapsed,
            status_extra,
            venue_id,
            venue_name,
            venue_city,
            league_round,
        ),
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

    ✅ 변경:
    - "DB에 뭔가 저장됨"이 아니라,
      "필터에 쓸 만큼 라인업이 실제로 유의미하게 채워짐"일 때만 True 반환.
      (대부분 startXI 11명이 들어오면 유의미하다고 판단)

    추가:
    - 런타임 캐시에 team별 player_id set 저장(players_by_team)
    - 라인업이 유의미하면 st["lineups_ready"]=True로 마킹(잠금 기준)
    """
    if not lineups_resp:
        return False

    def _extract_player_ids_and_counts(item: Dict[str, Any]) -> Tuple[List[int], int, int]:
        out: List[int] = []

        start_arr = item.get("startXI") or []
        sub_arr = item.get("substitutes") or []

        start_cnt = 0
        sub_cnt = 0

        if isinstance(start_arr, list):
            for row in start_arr:
                if not isinstance(row, dict):
                    continue
                p = row.get("player") or {}
                if not isinstance(p, dict):
                    continue
                pid = safe_int(p.get("id"))
                if pid is None:
                    continue
                out.append(pid)
                start_cnt += 1

        if isinstance(sub_arr, list):
            for row in sub_arr:
                if not isinstance(row, dict):
                    continue
                p = row.get("player") or {}
                if not isinstance(p, dict):
                    continue
                pid = safe_int(p.get("id"))
                if pid is None:
                    continue
                out.append(pid)
                sub_cnt += 1

        uniq = list(set(out))
        return uniq, start_cnt, sub_cnt

    updated_utc = iso_utc(updated_at)
    ok_any_write = False
    ready_any = False

    # state 준비
    st = _ensure_lineups_state(fixture_id)
    pb = st.get("players_by_team")
    if not isinstance(pb, dict):
        pb = {}
        st["players_by_team"] = pb

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
        ok_any_write = True

        # ---- 런타임 캐시 저장 + 유의미 판단 ----
        try:
            ids, start_cnt, sub_cnt = _extract_player_ids_and_counts(item)
            pb[team_id] = set(ids)

            # ✅ 유의미(ready) 기준:
            # - startXI가 11명 이상이면 거의 확정 라인업
            # - 혹은 추출 ids가 11명 이상(공급자 포맷 차이 방어)
            if (start_cnt >= 11) or (len(ids) >= 11):
                ready_any = True
        except Exception:
            # 캐시는 best-effort
            pass

    # 라인업이 유의미한 상태면 state에 ready 마킹(잠금 기준으로 사용)
    if ready_any:
        st["lineups_ready"] = True

    # DB write가 1번도 없으면 False
    if not ok_any_write:
        return False

    # ✅ 반환은 "유의미하게 준비됨" 여부
    return bool(ready_any)




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

    # ✅ success 잠금 조건 강화:
    # - success=True 이더라도 lineups_ready가 아니면(=불완전 라인업 가능성) 계속 시도 여지 남김
    if st.get("success") and st.get("lineups_ready"):
        return

    kickoff: Optional[dt.datetime] = None
    try:
        kickoff = dt.datetime.fromisoformat(date_utc.replace("Z", "+00:00"))
        if kickoff.tzinfo is None:
            kickoff = kickoff.replace(tzinfo=dt.timezone.utc)
        else:
            kickoff = kickoff.astimezone(dt.timezone.utc)
    except Exception:
        kickoff = None

    nowu = now.astimezone(dt.timezone.utc)

    # ---- 과호출 방지 쿨다운(초) ----
    # - UPCOMING(-60/-10)은 1회성 슬롯이므로 여기서는 쿨다운으로 막지 않는다.
    # - INPLAY 재시도 구간에서만(last_try_ts 기반) 쿨다운을 적용한다.
    COOLDOWN_SEC = 20


    # ─────────────────────────────────────
    # UPCOMING: -60 / -10 슬롯은 1회만
    # ─────────────────────────────────────
    if kickoff and status_group == "UPCOMING":
        mins = int((kickoff - nowu).total_seconds() / 60)

        # -60 슬롯: 59~61분 사이
        if (59 <= mins <= 61) and not st.get("slot60"):
            st["slot60"] = True
            try:
                # ✅ UPCOMING 슬롯은 INPLAY 쿨다운을 막지 않도록 last_try_ts를 찍지 않는다
                resp = fetch_lineups(session, fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)

                # ✅ ready일 때만 success 잠금
                if ready:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} slot60 ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} slot60 err: {e}", file=sys.stderr)
            return


        # -10 슬롯: 9~11분 사이
        if (9 <= mins <= 11) and not st.get("slot10"):
            st["slot10"] = True
            try:
                # ✅ UPCOMING 슬롯은 INPLAY 쿨다운을 막지 않도록 last_try_ts를 찍지 않는다
                resp = fetch_lineups(session, fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)

                # ✅ ready일 때만 success 잠금
                if ready:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} slot10 ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} slot10 err: {e}", file=sys.stderr)
            return


        return

    # ─────────────────────────────────────
    # INPLAY: 초반에는 불완전 응답이 흔함 → elapsed<=15까지 쿨다운 두고 재시도
    # ─────────────────────────────────────
    if status_group == "INPLAY":
        el = elapsed if elapsed is not None else 0  # elapsed None이어도 0으로 보고 1회는 시도 가능

        # ✅ 기존 5분 → 15분까지 확장
        if 0 <= el <= 15:
            # 쿨다운 체크 (UPCOMING 슬롯과 달리 여기서는 적용)
            last_try = float(st.get("last_try_ts") or 0.0)
            if (time.time() - last_try) < COOLDOWN_SEC:
                return

            try:
                st["last_try_ts"] = time.time()
                resp = fetch_lineups(session, fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)

                if ready:
                    st["success"] = True  # ✅ ready일 때만 잠금
                print(f"      [lineups] fixture_id={fixture_id} inplay(el={el}) ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} inplay err: {e}", file=sys.stderr)



# ─────────────────────────────────────
# 메인 1회 실행
# ─────────────────────────────────────

def run_once() -> int:
    if not API_KEY:
        print("[live_status_worker] APIFOOTBALL_KEY(env) 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    league_ids = parse_live_leagues(LIVE_LEAGUES_ENV)
    if not league_ids:
        print("[live_status_worker] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    # ✅ DDL은 워커 시작 시 1회만
    if not hasattr(run_once, "_ddl_done"):
        ensure_match_live_state_table()
        ensure_match_postmatch_timeline_state_table()
        run_once._ddl_done = True  # type: ignore[attr-defined]

    now = now_utc()
    fetched_at = now

    s = _session()

    # ─────────────────────────────────────
    # (0) live=all 감지(10초마다 1회)
    # ─────────────────────────────────────
    live_items: List[Dict[str, Any]] = []
    try:
        live_items = fetch_live_all(s)
    except Exception as e:
        print(f"[live_detect] err: {e}", file=sys.stderr)
        live_items = []

    # ✅ 날짜 범위 결정은 live=all 이후에 한다 (호출 증가 최소화 + 필요한 날짜만 추가)
    # - watched_live(필터된 라이브) 기준으로 넣는 게 가장 안전/절약
    # - 아직 watched_live가 아래에서 만들어지니, 여기선 일단 live_items 전체를 넣고
    #   실제 스캔 리그는 어차피 watched league로 제한되어 호출 폭증은 없음
    dates = target_dates_for_live(live_items)


    watched = set(league_ids)
    watched_live: List[Dict[str, Any]] = []
    live_league_set: set[int] = set()

    for it in live_items or []:
        lg = it.get("league") or {}
        lid = safe_int(lg.get("id"))
        if lid is None:
            continue
        if lid in watched:
            watched_live.append(it)
            live_league_set.add(lid)

    now_ts = time.time()

    # ✅ 최근 라이브 리그 TTL 갱신/정리
    for lid in live_league_set:
        RECENT_LIVE_LEAGUE_TS[lid] = now_ts
    for lid, ts in list(RECENT_LIVE_LEAGUE_TS.items()):
        if (now_ts - float(ts or 0.0)) > RECENT_LIVE_LEAGUE_TTL_SEC:
            RECENT_LIVE_LEAGUE_TS.pop(lid, None)

    idle_mode = (len(live_league_set) == 0)

    # ✅ 핵심 변경:
    # "라이브 리그만 스캔"하지 않고, watched 리그 전체는 항상 스캔 유지
    live_league_ids = list(league_ids)

    if idle_mode:
        print(f"[live_detect] watched_live=0 (live_all={len(live_items)}) → idle slow scan (leagues={len(live_league_ids)})")
    else:
        print(f"[live_detect] watched_live_leagues={len(live_league_set)} (live_all={len(live_items)}) → scan all watched leagues={len(live_league_ids)}")


    # ─────────────────────────────────────
    # (1) league/date 시즌 & 무경기 캐시 (API 낭비 감소)
    # ─────────────────────────────────────
    if not hasattr(run_once, "_fixtures_cache"):
        run_once._fixtures_cache = {}  # type: ignore[attr-defined]
    fc: Dict[Tuple[int, str], Dict[str, Any]] = run_once._fixtures_cache  # type: ignore[attr-defined]

    SEASON_TTL = 60 * 60

    # ✅ NOFIX를 바로 10분 박제하면 "일시적 빈 응답"에 FT를 놓칠 수 있음
    # - 1~2번 빈 응답은 흔들림으로 보고 짧게 재시도
    # - 연속 3번 이상일 때만 nofix로 취급
    NOFIX_TTL_SOFT = 60        # 1분: 재시도 유도
    NOFIX_TTL_HARD = 60 * 10   # 10분: 정말 없을 때만


    now_ts = time.time()
    for k, v in list(fc.items()):
        if float(v.get("exp") or 0) < now_ts:
            del fc[k]

    total_fixtures = 0
    total_inplay = 0

    fast_leagues = set(parse_fast_leagues(FAST_LEAGUES_ENV))

    fixture_groups: Dict[int, str] = {}

    for date_str in dates:
        for lid in live_league_ids:

            if idle_mode:
                scan_interval = IDLE_SCAN_INTERVAL_SEC
            else:
                # ✅ 라이브 중이거나 "최근 라이브였던" 리그는 빠르게 스캔
                recent_live = float(RECENT_LIVE_LEAGUE_TS.get(lid) or 0.0)
                is_hot = (lid in live_league_set) or ((now_ts - recent_live) <= RECENT_LIVE_LEAGUE_TTL_SEC)

                if is_hot:
                    scan_interval = 5 if lid in fast_leagues else DEFAULT_SCAN_INTERVAL_SEC
                else:
                    # ✅ 지금은 라이브가 아니어도 FT/포스트매치 반영을 위해 느리게는 계속 스캔
                    scan_interval = IDLE_SCAN_INTERVAL_SEC


            k_scan = (lid, date_str)
            last_scan = float(LAST_FIXTURES_SCAN_TS.get(k_scan) or 0.0)
            if scan_interval > 0 and (now_ts - last_scan) < scan_interval:
                continue
            LAST_FIXTURES_SCAN_TS[k_scan] = now_ts

            fixtures: List[Dict[str, Any]] = []
            used_season: Optional[int] = None

            cache_key = (lid, date_str)

            # ✅ 이 리그가 "라이브/최근라이브"면 nofix 박제 금지
            recent_live = float(RECENT_LIVE_LEAGUE_TS.get(lid) or 0.0)
            is_hot = (lid in live_league_set) or ((now_ts - recent_live) <= RECENT_LIVE_LEAGUE_TTL_SEC)

            cached = fc.get(cache_key)
            if cached and float(cached.get("exp") or 0) >= now_ts:
                # miss 카운트(연속 빈 응답)
                miss = int(cached.get("miss") or 0)

                # ✅ nofix는 "연속 3회 이상" + "hot 아님"일 때만 스킵
                if (cached.get("no") is True) and (miss >= 3) and (not is_hot):
                    continue

                cached_season = cached.get("season")
                if isinstance(cached_season, int):
                    try:
                        rows = fetch_fixtures(s, lid, date_str, cached_season)
                        if rows:
                            fixtures = rows
                            used_season = cached_season
                            # ✅ 성공하면 miss 리셋
                            fc[cache_key] = {"season": cached_season, "no": False, "miss": 0, "exp": now_ts + SEASON_TTL}
                        else:
                            # ✅ 빈 응답이면 miss 증가(일시 흔들림 가능)
                            miss2 = miss + 1
                            # hot이면 hard 박제 금지(짧게 재시도)
                            exp = now_ts + (NOFIX_TTL_SOFT if is_hot else (NOFIX_TTL_HARD if miss2 >= 3 else NOFIX_TTL_SOFT))
                            fc[cache_key] = {"season": cached_season, "no": (miss2 >= 3 and not is_hot), "miss": miss2, "exp": exp}
                    except Exception as e:
                        # ✅ 에러는 nofix로 박제하지 말고 캐시 제거(다음 루프 재시도)
                        fc.pop(cache_key, None)
                        print(f"  [fixtures] league={lid} date={date_str} season={cached_season} err: {e}", file=sys.stderr)

            if used_season is None:
                found_any = False
                last_err = None
                for season in infer_season_candidates(date_str):
                    try:
                        rows = fetch_fixtures(s, lid, date_str, season)
                        if rows:
                            fixtures = rows
                            used_season = season
                            found_any = True
                            fc[cache_key] = {"season": season, "no": False, "miss": 0, "exp": now_ts + SEASON_TTL}
                            break
                    except Exception as e:
                        last_err = e
                        print(f"  [fixtures] league={lid} date={date_str} season={season} err: {e}", file=sys.stderr)

                if not found_any and used_season is None:
                    # ✅ 어떤 시즌도 못 찾았는데, 이게 '진짜 무경기'인지 '일시 빈 응답'인지 구분이 필요
                    prev = fc.get(cache_key) or {}
                    miss = int(prev.get("miss") or 0) + 1

                    # hot이면 절대 hard nofix로 박제 금지(짧게 재시도)
                    exp = now_ts + (NOFIX_TTL_SOFT if is_hot else (NOFIX_TTL_HARD if miss >= 3 else NOFIX_TTL_SOFT))
                    fc[cache_key] = {"season": None, "no": (miss >= 3 and not is_hot), "miss": miss, "exp": exp}

                    # ✅ 지금은 스킵 (다음 run에서 다시 시도하게 됨)
                    continue


            total_fixtures += len(fixtures)
            print(f"[fixtures] league={lid} date={date_str} season={used_season} count={len(fixtures)}")

            for item in fixtures:
                try:
                    fx = item.get("fixture") or {}
                    fid = safe_int(fx.get("id"))
                    if fid is None:
                        continue

                    st = fx.get("status") or {}
                    status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
                    status_group = map_status_group(status_short)
                    fixture_groups[fid] = status_group

                    upsert_fixture_row(
                        fixture_id=fid,
                        league_id=lid,
                        season=used_season,
                        date_utc=safe_text(fx.get("date")),
                        status_short=status_short,
                        status_group=status_group,
                    )

                    fixture_id, home_id, away_id, sg, date_utc = upsert_match_row_from_fixture(
                        item, league_id=lid, season=used_season
                    )

                    try:
                        upsert_match_fixtures_raw(fixture_id, item, fetched_at)
                    except Exception as raw_err:
                        print(f"      [match_fixtures_raw] fixture_id={fixture_id} err: {raw_err}", file=sys.stderr)

                    try:
                        elapsed = safe_int((item.get("fixture") or {}).get("status", {}).get("elapsed"))
                        maybe_sync_lineups(s, fixture_id, date_utc, sg, elapsed, now)
                    except Exception as lu_err:
                        print(f"      [lineups] fixture_id={fixture_id} policy err: {lu_err}", file=sys.stderr)

                    # ✅ FINISHED: FT 이후 2회 덮어쓰기 정책 유지
                    try:
                        maybe_sync_postmatch_timeline(s, fixture_id, sg, now)
                    except Exception as pm_err:
                        print(f"      [postmatch_timeline] fixture_id={fixture_id} err: {pm_err}", file=sys.stderr)

                    # INPLAY 처리
                    if sg != "INPLAY":
                        continue

                    total_inplay += 1

                    # ✅ (1) events 스냅샷 미러링: 워커 주기마다 그대로 DB에 반영 (쿨다운 제거)
                    try:
                        events = fetch_events(s, fixture_id)

                        # raw 저장(best-effort)
                        try:
                            upsert_match_events_raw(fixture_id, events, now)
                        except Exception:
                            pass

                        # fixture 단위 전체 교체(DELETE -> INSERT)
                        inserted = 0
                        try:
                            inserted = replace_match_events_for_fixture(fixture_id, events)
                        except Exception:
                            inserted = 0

                        # red 요약도 동일 스냅샷에서 계산
                        try:
                            h_red, a_red = calc_red_cards_from_events(events, home_id, away_id)
                            upsert_match_live_state(fixture_id, h_red, a_red, now)
                        except Exception:
                            pass

                        print(f"      [events_snapshot] fixture_id={fixture_id} events={len(events)} inserted={inserted}")

                    except Exception as ev_err:
                        print(f"      [events_snapshot] fixture_id={fixture_id} err: {ev_err}", file=sys.stderr)

                    # (2) stats (60초 쿨다운 유지)
                    try:
                        now_ts3 = time.time()
                        last_ts = LAST_STATS_SYNC.get(fixture_id)
                        if (last_ts is None) or ((now_ts3 - last_ts) >= STATS_INTERVAL_SEC):
                            stats = fetch_team_stats(s, fixture_id)
                            upsert_match_team_stats(fixture_id, stats)
                            LAST_STATS_SYNC[fixture_id] = now_ts3
                            print(f"      [stats] fixture_id={fixture_id} updated")
                    except Exception as st_err:
                        print(f"      [stats] fixture_id={fixture_id} err: {st_err}", file=sys.stderr)

                except Exception as e:
                    print(f"  ! fixture 처리 중 에러: {e}", file=sys.stderr)

    # ─────────────────────────────────────
    # (6) 런타임 캐시 prune (메모리 누적 방지)
    # ─────────────────────────────────────
    try:
        for fid, g in list(fixture_groups.items()):
            if g in ("FINISHED", "OTHER"):
                LAST_STATS_SYNC.pop(fid, None)
                LINEUPS_STATE.pop(fid, None)

        if len(LINEUPS_STATE) > 3000:
            for fid in list(LINEUPS_STATE.keys())[: len(LINEUPS_STATE) - 2000]:
                LINEUPS_STATE.pop(fid, None)
    except Exception:
        pass

    print(f"[live_status_worker] done. total_fixtures={total_fixtures}, inplay={total_inplay}")
    return total_inplay





# ─────────────────────────────────────
# 루프
# ─────────────────────────────────────

def loop() -> None:
    print(
        f"[live_status_worker] start (detect_interval={DETECT_INTERVAL_SEC}s, default_scan={DEFAULT_SCAN_INTERVAL_SEC}s, fast_leagues_env='{FAST_LEAGUES_ENV}')"
    )
    while True:
        try:
            run_once()
        except Exception:
            traceback.print_exc()
        # ✅ 항상 감지 주기로만 돈다 (IDLE 60초 같은 모드 없음)
        time.sleep(DETECT_INTERVAL_SEC)



if __name__ == "__main__":
    loop()

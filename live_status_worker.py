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
import re
import traceback
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Set

import requests

from db import execute, fetch_all  # dev 스키마 확정 → 런타임 schema 조회 불필요




# ─────────────────────────────────────
# ENV / 상수
# ─────────────────────────────────────

API_KEY = (
    os.environ.get("APISPORTS_KEY")
    or os.environ.get("APIFOOTBALL_KEY")
    or os.environ.get("API_FOOTBALL_KEY")
)
LIVE_LEAGUES_ENV = os.environ.get("LIVE_LEAGUES", "")

# ✅ (A) 라이브 감지 주기: live=all 호출 간격(초)
DETECT_INTERVAL_SEC = int(os.environ.get("LIVE_DETECT_INTERVAL_SEC", "10"))

# ✅ (B) 리그별 스캔 모드(/fixtures league/date 스캔) 기본 간격(초)
DEFAULT_SCAN_INTERVAL_SEC = int(os.environ.get("DEFAULT_LIVE_FIXTURES_INTERVAL_SEC", "10"))

# ✅ (B-2) 라이브가 0일 때(=watched_live=0)에도 "종료 반영/포스트매치"를 위해 느리게 스캔(초)
# 기본 90초(60~120 권장). 필요하면 ENV로 조절.
IDLE_SCAN_INTERVAL_SEC = int(os.environ.get("IDLE_LIVE_FIXTURES_INTERVAL_SEC", "90"))

# ✅ (D) date(league/date/season) 스캔은 "어제/오늘/내일"만 1시간에 1번(백필/FT 보정용)
DATE_SCAN_INTERVAL_SEC = int(os.environ.get("LIVE_DATE_SCAN_INTERVAL_SEC", "3600"))

# ✅ (D-2) backfill B안: run_once에서 backfill이 live 틱을 밀지 않게 "처리 예산" 제한
BACKFILL_BUDGET_SEC = float(os.environ.get("LIVE_BACKFILL_BUDGET_SEC", "2.5"))   # run_once당 backfill에 쓸 최대 시간
BACKFILL_MAX_COMBOS = int(os.environ.get("LIVE_BACKFILL_MAX_COMBOS", "8"))       # run_once당 (date,lid) 최대 처리 개수


# ✅ (E) 워치독: DB에 오래 남은 INPLAY를 60초마다 단건 조회로 FT 보정
WATCHDOG_INTERVAL_SEC = int(os.environ.get("LIVE_WATCHDOG_INTERVAL_SEC", "60"))
WATCHDOG_STALE_HOURS = float(os.environ.get("LIVE_WATCHDOG_STALE_HOURS", "2"))
WATCHDOG_LIMIT = int(os.environ.get("LIVE_WATCHDOG_LIMIT", "50"))

# ✅ 예정 경기 재검증: DB에 저장된 NS/TBD/PST/UPCOMING 경기를 순환 조회
# - backfill 루프에서만 사용
# - 전체를 한 번에 치지 않고, LIMIT 개수씩 커서 순환
SCHEDULE_RECHECK_LIMIT = int(os.environ.get("SCHEDULE_RECHECK_LIMIT", "50"))


# ✅ (C) “핵심 리그는 5초” 같은 오버라이드 목록
# 예) "39,140,135"  (여기에 포함된 리그만 5초)
FAST_LEAGUES_ENV = os.environ.get("FAST_LIVE_LEAGUES", "")


# (구버전 호환: 기존 INTERVAL_SEC는 더 이상 루프 sleep에 직접 쓰지 않음)
INTERVAL_SEC = int(os.environ.get("LIVE_WORKER_INTERVAL_SEC", str(DETECT_INTERVAL_SEC)))


BASE = "https://v3.football.api-sports.io"
UA = "SportsStatsX-LiveWorker/1.0"

STATS_INTERVAL_SEC = int(os.environ.get("LIVE_STATS_INTERVAL_SEC", "60"))   # stats 쿨다운
EVENTS_INTERVAL_SEC = int(os.environ.get("LIVE_EVENTS_INTERVAL_SEC", "15"))  # events 쿨다운
LIVE_HOTPATH_BUDGET_SEC = float(os.environ.get("LIVE_HOTPATH_BUDGET_SEC", "6.0"))  # live hot path 예산
LIVE_SLOW_API_LOG_SEC = float(os.environ.get("LIVE_SLOW_API_LOG_SEC", "2.0"))       # slow api log threshold
REQ_TIMEOUT = int(os.environ.get("LIVE_REQ_TIMEOUT_SEC", "12"))
REQ_RETRIES = int(os.environ.get("LIVE_REQ_RETRIES", "2"))




# ─────────────────────────────────────
# 런타임 캐시
# ─────────────────────────────────────

LAST_STATS_SYNC: Dict[int, float] = {}   # fixture_id -> last ts
LAST_EVENTS_SYNC: Dict[int, float] = {}  # fixture_id -> last ts
LINEUPS_STATE: Dict[int, Dict[str, Any]] = {}  # fixture_id -> {"slot60":bool,"slot10":bool,"success":bool}

# ✅ 리그별 스캔 모드에서 /fixtures(league/date) 호출 간격 제어
LAST_FIXTURES_SCAN_TS: Dict[Tuple[int, str], float] = {}

# ✅ 워치독 실행 타이밍
LAST_WATCHDOG_TS: float = 0.0

# ✅ backfill B안: (date,lid) 조합을 run_once마다 분할 처리하기 위한 커서
BACKFILL_CURSOR: int = 0

# ✅ 예정 경기 전체 순환 재검증용 커서
SCHEDULE_RECHECK_CURSOR: int = 0







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

def target_dates_for_scan() -> List[str]:
    """
    ✅ date 스캔은 "어제/오늘/내일" 고정.
    - 목적: FT 반영 지연/누락 백필(너가 원한 정책)
    - 주기: DATE_SCAN_INTERVAL_SEC 로 1시간 1번만 돌게 제어
    """
    now = now_utc()
    today = now.date()
    return [
        (today - dt.timedelta(days=1)).isoformat(),
        today.isoformat(),
        (today + dt.timedelta(days=1)).isoformat(),
    ]



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
    - ✅ 느린 API 호출 slow log 추가
    """
    url = f"{BASE}{path}"

    if not hasattr(api_get, "_rl"):
        try:
            per_min = float(os.environ.get("RATE_LIMIT_PER_MIN", "0") or "0")
        except Exception:
            per_min = 0.0
        try:
            burst = float(os.environ.get("RATE_LIMIT_BURST", "0") or "0")
        except Exception:
            burst = 0.0

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
            return

        now_ts = time.time()
        last_ts = float(rl.get("ts") or now_ts)
        elapsed = max(0.0, now_ts - last_ts)

        tokens = float(rl.get("tokens") or 0.0) + elapsed * rate
        if tokens > max_t:
            tokens = max_t
        rl["tokens"] = tokens
        rl["ts"] = now_ts

        if tokens >= 1.0:
            rl["tokens"] = tokens - 1.0
            return

        need = 1.0 - tokens
        wait_sec = need / rate if rate > 0 else 0.25
        if wait_sec > 0:
            time.sleep(wait_sec)

        now_ts2 = time.time()
        elapsed2 = max(0.0, now_ts2 - float(rl.get("ts") or now_ts2))
        tokens2 = float(rl.get("tokens") or 0.0) + elapsed2 * rate
        if tokens2 > max_t:
            tokens2 = max_t
        rl["tokens"] = max(0.0, tokens2 - 1.0)
        rl["ts"] = now_ts2

    last_err: Optional[Exception] = None
    for attempt in range(REQ_RETRIES + 1):
        started = time.time()
        try:
            _acquire_token()
            r = session.get(url, params=params, timeout=REQ_TIMEOUT)
            elapsed = time.time() - started

            if elapsed >= LIVE_SLOW_API_LOG_SEC:
                print(
                    f"[slow_api] path={path} elapsed={elapsed:.2f}s status={r.status_code} params={params}",
                    flush=True,
                )

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
            elapsed = time.time() - started
            last_err = e
            print(
                f"[api_err] path={path} attempt={attempt + 1}/{REQ_RETRIES + 1} elapsed={elapsed:.2f}s params={params} err={e}",
                file=sys.stderr,
                flush=True,
            )
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

def fetch_fixture_by_id(session: requests.Session, fixture_id: int) -> Optional[Dict[str, Any]]:
    """
    ✅ 워치독용: fixture 단건 조회
    API-Sports: /fixtures?id=XXXX
    """
    data = api_get(session, "/fixtures", {"id": fixture_id})
    resp = (data.get("response") or []) if isinstance(data, dict) else []
    if resp and isinstance(resp, list) and isinstance(resp[0], dict):
        return resp[0]
    return None


def recheck_scheduled_fixtures(
    session: requests.Session,
    fetched_at: dt.datetime,
    limit: int = 50,
) -> int:
    """
    ✅ 예정 경기 전체 순환 재검증
    목적:
    - 기존 date 기반 backfill만으로는 "미래로 멀리 밀린 경기"를 다시 못 잡는 문제 보완
    - DB에 이미 저장된 예정 경기(NS/TBD/PST/UPCOMING)를 fixture_id 단건조회로 재확인
    - 전체를 한 번에 치지 않고, LIMIT씩 커서 순환

    주의:
    - 기존 기능 변경 없음
    - 라이브/이벤트/라인업 로직 건드리지 않음
    - date/status/raw 정합성만 다시 맞춤
    """
    global SCHEDULE_RECHECK_CURSOR

    page_size = max(1, int(limit or 50))
    offset = max(0, int(SCHEDULE_RECHECK_CURSOR or 0))

    def _select_page(off: int) -> List[Dict[str, Any]]:
        return fetch_all(
            """
            SELECT fixture_id, league_id, season
            FROM matches
            WHERE
                status_group = 'UPCOMING'
                OR status IN ('NS', 'TBD', 'PST')
            ORDER BY NULLIF(date_utc,'')::timestamptz ASC NULLS LAST, fixture_id ASC
            LIMIT %s OFFSET %s
            """,
            (page_size, off),
        ) or []

    rows = _select_page(offset)

    # 커서가 끝에 도달했으면 처음부터 다시 순환
    if not rows and offset > 0:
        offset = 0
        rows = _select_page(offset)

    if not rows:
        SCHEDULE_RECHECK_CURSOR = 0
        print("      [schedule_recheck] candidates=0")
        return 0

    processed = 0

    for r in rows:
        fid = safe_int(r.get("fixture_id"))
        lid = safe_int(r.get("league_id"))
        season = safe_int(r.get("season"))

        if fid is None or lid is None or season is None:
            continue

        try:
            fx_obj = fetch_fixture_by_id(session, fid)
            if not fx_obj:
                continue

            lg = fx_obj.get("league") or {}
            fx = fx_obj.get("fixture") or {}
            st = fx.get("status") or {}

            real_lid = safe_int(lg.get("id")) or lid
            real_season = safe_int(lg.get("season")) or season
            status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
            status_group = map_status_group(status_short)

            upsert_fixture_row(
                fixture_id=fid,
                league_id=real_lid,
                season=real_season,
                date_utc=safe_text(fx.get("date")),
                status_short=status_short,
                status_group=status_group,
            )

            upsert_match_row_from_fixture(
                fx_obj,
                league_id=real_lid,
                season=real_season,
            )

            try:
                upsert_match_fixtures_raw(fid, fx_obj, fetched_at)
            except Exception:
                pass

            processed += 1

        except Exception as e:
            print(f"      [schedule_recheck] fixture_id={fid} err: {e}", file=sys.stderr)

    next_offset = offset + len(rows)
    SCHEDULE_RECHECK_CURSOR = next_offset

    print(f"      [schedule_recheck] scanned={len(rows)} processed={processed} next_offset={SCHEDULE_RECHECK_CURSOR}")
    return processed


def watchdog_fix_stale_inplay(session: requests.Session, now: dt.datetime) -> int:
    """
    ✅ 60초마다 1회:
    - DB에는 INPLAY인데, 실제 API는 FT(또는 OTHER)로 끝난 경기들을 단건 조회로 보정
    - 호출량 폭발 방지: LIMIT + 워치독 주기
    반환: 처리한 fixture 수(시도 기준)
    """
    global LAST_WATCHDOG_TS

    now_ts = time.time()
    if (now_ts - float(LAST_WATCHDOG_TS or 0.0)) < WATCHDOG_INTERVAL_SEC:
        return 0
    LAST_WATCHDOG_TS = now_ts

    # "오래된 INPLAY" 후보를 DB에서 뽑는다
    # - elapsed>=85 OR kickoff 기준 WATCHDOG_STALE_HOURS 이상 지난 경우
    # - date_utc는 text라 NULLIF 후 timestamptz 캐스팅
    rows = fetch_all(
        """
        SELECT fixture_id, league_id, season, date_utc, elapsed
        FROM matches
        WHERE status_group = 'INPLAY'
          AND (
            (elapsed IS NOT NULL AND elapsed >= 85)
            OR (
              NULLIF(date_utc,'')::timestamptz < (NOW() - (%s || ' hours')::interval)
            )
          )
        ORDER BY NULLIF(date_utc,'')::timestamptz ASC NULLS LAST
        LIMIT %s
        """,
        (str(WATCHDOG_STALE_HOURS), int(WATCHDOG_LIMIT)),
    )

    if not rows:
        print("      [watchdog] candidates=0")
        return 0

    fixed = 0
    tried = 0

    for r in rows:
        fid = safe_int(r.get("fixture_id"))
        if fid is None:
            continue

        tried += 1

        try:
            fx_obj = fetch_fixture_by_id(session, fid)
            if not fx_obj:
                continue

            lg = fx_obj.get("league") or {}
            lid = safe_int(lg.get("id")) or safe_int(r.get("league_id"))
            season = safe_int(lg.get("season")) or safe_int(r.get("season"))

            if lid is None or season is None:
                # upsert_match_row_from_fixture는 league_id/season 필수라서, 둘 다 없으면 스킵
                continue

            fx = fx_obj.get("fixture") or {}
            st = fx.get("status") or {}
            status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
            sg = map_status_group(status_short)

            # fixtures 테이블도 같이 정합
            upsert_fixture_row(
                fixture_id=fid,
                league_id=lid,
                season=season,
                date_utc=safe_text(fx.get("date")),
                status_short=status_short,
                status_group=sg,
            )

            # matches 테이블 보정(FT/home_ft/away_ft/elapsed 포함)
            fixture_id, home_id, away_id, sg2, date_utc = upsert_match_row_from_fixture(
                fx_obj, league_id=lid, season=season
            )

            # raw도 best-effort
            try:
                upsert_match_fixtures_raw(fixture_id, fx_obj, now)
            except Exception:
                pass

            # ✅ FT로 바뀐 경우: postmatch timeline 정책 실행(60초/30분은 state로 제어됨)
            if sg2 == "FINISHED":
                try:
                    maybe_sync_postmatch_timeline(session, fixture_id, sg2, now)
                except Exception:
                    pass

                # ✅ FT 트리거 기록(B안)
                try:
                    enqueue_ft_trigger(fixture_id, lid, season, finished_iso_utc=iso_utc(now))
                except Exception:
                    pass

                fixed += 1


        except Exception as e:
            print(f"      [watchdog] fixture_id={fid} err: {e}", file=sys.stderr)

    print(f"      [watchdog] candidates={len(rows)} tried={tried} fixed_to_finished={fixed}")
    return tried



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

# ─────────────────────────────────────
# FT 트리거(B 소비) + Standings/Bracket 스키마 자동 추가
# - 어디서 FT가 감지되든( live / backfill / watchdog ) 트리거 1회 기록
# - standings/bracket 워커는 트리거를 소비(B안)해서 "FT 직후 1회" 실행
# - TTL 90일 정리(트리거/브라켓)
# ─────────────────────────────────────

FT_TRIGGER_TTL_DAYS = int(os.environ.get("FT_TRIGGER_TTL_DAYS", "90"))

STANDINGS_LOOP_SEC = int(os.environ.get("STANDINGS_LOOP_SEC", "1800"))  # 30분
TRIGGER_POLL_SEC   = int(os.environ.get("FT_TRIGGER_POLL_SEC", "10"))   # standings 트리거 폴링 간격(짧게)


def ensure_ft_triggers_table() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS ft_triggers (
            fixture_id               integer PRIMARY KEY,
            league_id                integer NOT NULL,
            season                   integer NOT NULL,
            finished_utc             text,
            standings_consumed_utc   text,
            created_utc              text,
            updated_utc              text
        )
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_ft_triggers_created_utc ON ft_triggers (created_utc)")
    execute("CREATE INDEX IF NOT EXISTS idx_ft_triggers_league_season ON ft_triggers (league_id, season)")


def ensure_competition_structure_tables() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS competition_api_raw (
            league_id   integer NOT NULL,
            season      integer NOT NULL,
            endpoint    text    NOT NULL,
            data_json   text    NOT NULL,
            fetched_at  text,
            updated_at  text,
            PRIMARY KEY (league_id, season, endpoint)
        )
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS competition_season_meta (
            league_id                         integer NOT NULL,
            season                            integer NOT NULL,
            league_name                       text,
            league_type                       text,
            country_name                      text,
            season_start                      text,
            season_end                        text,
            season_current                    integer,
            coverage_standings                integer,
            coverage_events                   integer,
            coverage_lineups                  integer,
            coverage_statistics_fixtures      integer,
            coverage_players                  integer,
            has_standings                     integer,
            standings_rows                    integer,
            has_groups                        integer,
            groups_count                      integer,
            has_rounds                        integer,
            rounds_count                      integer,
            has_knockout_rounds               integer,
            format_hint                       text,
            updated_utc                       text,
            PRIMARY KEY (league_id, season)
        )
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS standings_group_meta (
            league_id             integer NOT NULL,
            season                integer NOT NULL,
            group_name            text    NOT NULL,
            group_order           integer,
            group_kind            text,
            is_primary            integer,
            table_rows            integer,
            description_summary   text,
            raw_json              text,
            updated_utc           text,
            PRIMARY KEY (league_id, season, group_name)
        )
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS competition_rounds_meta (
            league_id     integer NOT NULL,
            season        integer NOT NULL,
            round_name    text    NOT NULL,
            round_order   integer,
            round_kind    text,
            is_knockout   integer,
            raw_json      text,
            updated_utc   text,
            PRIMARY KEY (league_id, season, round_name)
        )
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_competition_api_raw_endpoint ON competition_api_raw (endpoint)")
    execute("CREATE INDEX IF NOT EXISTS idx_standings_group_meta_lookup ON standings_group_meta (league_id, season, group_order)")
    execute("CREATE INDEX IF NOT EXISTS idx_competition_rounds_meta_lookup ON competition_rounds_meta (league_id, season, round_order)")


def _json_compact(payload: Any) -> str:
    return json.dumps(payload if payload is not None else {}, ensure_ascii=False, separators=(",", ":"))


def _bool_to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    return 1 if bool(v) else 0


def _norm_lower(v: Any) -> str:
    if v is None:
        return ""
    try:
        return " ".join(str(v).strip().lower().split())
    except Exception:
        return ""




def _infer_round_kind(round_name: Optional[str]) -> str:
    r = _norm_lower(round_name)

    if not r:
        return "unknown"

    # ─────────────────────────
    # 컵 라운드 (가장 구체적인 것부터)
    # ─────────────────────────

    # Final
    if re.search(r"\bfinal\b", r) and "semi" not in r and "quarter" not in r:
        return "final"

    # Semi Final
    if "semi-final" in r or "semi finals" in r or "semi-final" in r:
        return "semi_final"

    # Quarter Final
    if "quarter-final" in r or "quarter finals" in r:
        return "quarter_final"

    # Round of X
    m = re.search(r"round of\s*(\d+)", r)
    if m:
        n = int(m.group(1))
        if n == 16:
            return "round_of_16"
        if n == 32:
            return "round_of_32"
        if n == 64:
            return "round_of_64"
        return "knockout_round"

    # 1/128-finals 같은 패턴
    m = re.search(r"1/\s*(\d+)", r)
    if m:
        return "knockout_round"

    # ─────────────────────────
    # Qualifying / Preliminary
    # ─────────────────────────

    if "extra preliminary" in r:
        return "qualifying_round"

    if "preliminary" in r:
        return "qualifying_round"

    if "qualifying" in r:
        return "qualifying_round"

    # ─────────────────────────
    # League structures
    # ─────────────────────────

    if "league stage" in r:
        return "league_phase"

    if "regular season" in r:
        return "regular_round"

    if "1st phase" in r or "2nd phase" in r:
        return "phase_round"

    if "championship round" in r:
        return "championship_round"

    if "relegation round" in r:
        return "relegation_round"

    # ─────────────────────────
    # Playoff
    # ─────────────────────────

    if "play-in" in r:
        return "play_in"

    if "playoff" in r or "play-off" in r or "play-offs" in r:
        return "playoff"

    # fallback
    return "other_round"


def _infer_round_is_knockout(round_name: Optional[str]) -> int:
    k = _infer_round_kind(round_name)

    return 1 if k in {
        "qualifying_round",
        "knockout_round",
        "round_of_64",
        "round_of_32",
        "round_of_16",
        "quarter_final",
        "semi_final",
        "final",
        "play_in",
        "playoff",
    } else 0

def _extract_round_number(round_name: Optional[str]) -> Optional[int]:
    if not isinstance(round_name, str):
        return None
    text = round_name.strip()
    if not text:
        return None

    m = re.search(r"(\d+)\s*$", text)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    m = re.search(r"regular season\s*-\s*(\d+)", text, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    return None


def _round_sort_key(round_name: Optional[str]) -> Tuple[int, int, str]:
    rn = safe_text(round_name) or ""
    kind = _infer_round_kind(rn)
    num = _extract_round_number(rn)

    priority_map = {
        "qualifying_round": 10,
        "phase_round": 20,
        "regular_round": 30,
        "league_phase": 40,
        "other_round": 50,
        "play_in": 60,
        "playoff": 70,
        "round_of_64": 80,
        "round_of_32": 90,
        "round_of_16": 100,
        "quarter_final": 110,
        "semi_final": 120,
        "final": 130,
        "unknown": 999,
    }

    pri = priority_map.get(kind, 999)

    if num is None:
        num = 999999

    return (pri, num, rn.lower())


def _sort_rounds(rounds: List[str]) -> List[str]:
    uniq: List[str] = []
    seen = set()

    for r in rounds or []:
        name = (safe_text(r) or "").strip()
        if not name:
            continue

        key = name.lower()
        if key in seen:
            continue

        seen.add(key)
        uniq.append(name)

    return uniq


def upsert_competition_api_raw(
    league_id: int,
    season: int,
    endpoint: str,
    payload: Any,
    fetched_at: dt.datetime,
) -> None:
    ts = iso_utc(fetched_at)
    raw = _json_compact(payload)
    execute(
        """
        INSERT INTO competition_api_raw (
            league_id, season, endpoint, data_json, fetched_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (league_id, season, endpoint) DO UPDATE SET
            data_json   = EXCLUDED.data_json,
            fetched_at  = EXCLUDED.fetched_at,
            updated_at  = EXCLUDED.updated_at
        WHERE
            competition_api_raw.data_json IS DISTINCT FROM EXCLUDED.data_json
        """,
        (int(league_id), int(season), str(endpoint), raw, ts, ts),
    )


def replace_rounds_for_league_season(league_id: int, season: int, rounds: List[str]) -> int:
    execute(
        "DELETE FROM rounds WHERE league_id = %s AND season = %s",
        (int(league_id), int(season)),
    )
    n = 0
    sorted_rounds = _sort_rounds(rounds or [])
    for round_name in sorted_rounds:
        execute(
            "INSERT INTO rounds (league_id, round, season) VALUES (%s, %s, %s)",
            (int(league_id), str(round_name), int(season)),
        )
        n += 1
    return n


def _group_description_summary(group_rows: List[Dict[str, Any]]) -> Optional[str]:
    vals: List[str] = []
    seen: Set[str] = set()
    for row in group_rows or []:
        desc = safe_text(row.get("description"))
        if not desc:
            continue
        desc = desc.strip()
        if not desc or desc in seen:
            continue
        seen.add(desc)
        vals.append(desc)
    if not vals:
        return None
    return " | ".join(vals[:20])


def replace_standings_group_meta(
    league_id: int,
    season: int,
    groups: List[Dict[str, Any]],
    updated_at: dt.datetime,
) -> int:
    execute(
        "DELETE FROM standings_group_meta WHERE league_id = %s AND season = %s",
        (int(league_id), int(season)),
    )

    prepared: List[Dict[str, Any]] = []
    for idx, grp in enumerate(groups or [], start=1):
        group_name = (safe_text(grp.get("group_name")) or f"Group {idx}").strip()
        group_rows = grp.get("rows") or []
        group_order = safe_int(grp.get("group_order")) or idx

        prepared.append(
            {
                "group_name": group_name,
                "group_order": group_order,
                "group_kind": None,
                "is_primary": 0,
                "table_rows": len(group_rows),
                "description_summary": _group_description_summary(group_rows),
                "raw_json": _json_compact(group_rows),
            }
        )

    updated_utc = iso_utc(updated_at)

    n = 0
    for item in prepared:
        execute(
            """
            INSERT INTO standings_group_meta (
                league_id, season, group_name, group_order, group_kind,
                is_primary, table_rows, description_summary, raw_json, updated_utc
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(league_id),
                int(season),
                item["group_name"],
                int(item["group_order"]),
                item["group_kind"],
                int(item["is_primary"]),
                int(item["table_rows"]),
                item["description_summary"],
                item["raw_json"],
                updated_utc,
            ),
        )
        n += 1
    return n


def replace_competition_rounds_meta(
    league_id: int,
    season: int,
    rounds: List[str],
    updated_at: dt.datetime,
) -> int:
    execute(
        "DELETE FROM competition_rounds_meta WHERE league_id = %s AND season = %s",
        (int(league_id), int(season)),
    )

    updated_utc = iso_utc(updated_at)
    n = 0
    sorted_rounds = _sort_rounds(rounds or [])

    for idx, round_name in enumerate(sorted_rounds, start=1):
        round_kind = _infer_round_kind(round_name)
        is_knockout = _infer_round_is_knockout(round_name)
        execute(
            """
            INSERT INTO competition_rounds_meta (
                league_id, season, round_name, round_order, round_kind,
                is_knockout, raw_json, updated_utc
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(league_id),
                int(season),
                str(round_name),
                int(idx),
                round_kind,
                int(is_knockout),
                _json_compact({"round_name": round_name}),
                updated_utc,
            ),
        )
        n += 1
    return n


def fetch_league_season_meta(session: requests.Session, league_id: int, season: int) -> Dict[str, Any]:
    data = api_get(session, "/leagues", {"id": league_id, "season": season})
    resp = (data.get("response") or []) if isinstance(data, dict) else []
    item = resp[0] if resp and isinstance(resp[0], dict) else {}
    return {"raw": data, "item": item}


def fetch_rounds(session: requests.Session, league_id: int, season: int) -> Dict[str, Any]:
    data = api_get(session, "/fixtures/rounds", {"league": league_id, "season": season})
    resp = (data.get("response") or []) if isinstance(data, dict) else []
    rounds = [str(x) for x in resp if isinstance(x, str) and str(x).strip()]
    return {"raw": data, "rounds": rounds}


def fetch_standings_bundle(session: requests.Session, league_id: int, season: int) -> Dict[str, Any]:
    data = api_get(session, "/standings", {"league": league_id, "season": season})
    resp = (data.get("response") or []) if isinstance(data, dict) else []

    rows: List[Dict[str, Any]] = []
    groups: List[Dict[str, Any]] = []

    if resp and isinstance(resp[0], dict):
        league_obj = resp[0].get("league") or {}
        standings = league_obj.get("standings") or []

        if isinstance(standings, list) and standings and isinstance(standings[0], list):
            for idx, group_rows in enumerate(standings, start=1):
                group_rows = [x for x in (group_rows or []) if isinstance(x, dict)]
                if not group_rows:
                    continue
                group_name = safe_text(group_rows[0].get("group")) or f"Group {idx}"
                rows.extend(group_rows)
                groups.append(
                    {
                        "group_name": group_name,
                        "group_order": idx,
                        "rows": group_rows,
                    }
                )
        elif isinstance(standings, list):
            flat_rows = [x for x in standings if isinstance(x, dict)]
            if flat_rows:
                group_name = safe_text(flat_rows[0].get("group")) or "Overall"
                rows.extend(flat_rows)
                groups.append(
                    {
                        "group_name": group_name,
                        "group_order": 1,
                        "rows": flat_rows,
                    }
                )

    return {
        "raw": data,
        "rows": rows,
        "groups": groups,
    }


def fetch_standings(session: requests.Session, league_id: int, season: int) -> List[Dict[str, Any]]:
    bundle = fetch_standings_bundle(session, league_id, season)
    return bundle.get("rows") or []


def upsert_competition_season_meta(
    league_id: int,
    season: int,
    league_meta_bundle: Dict[str, Any],
    standings_bundle: Dict[str, Any],
    rounds_bundle: Dict[str, Any],
    updated_at: dt.datetime,
) -> Dict[str, Any]:
    item = (league_meta_bundle or {}).get("item") or {}
    league_obj = item.get("league") or {}
    country_obj = item.get("country") or {}
    seasons = item.get("seasons") or []

    season_obj: Dict[str, Any] = {}
    for s in seasons:
        if safe_int((s or {}).get("year")) == int(season):
            season_obj = s or {}
            break

    cov = season_obj.get("coverage") or {}
    fixtures_cov = cov.get("fixtures") or {}

    rows = (standings_bundle or {}).get("rows") or []
    groups = (standings_bundle or {}).get("groups") or []
    rounds = (rounds_bundle or {}).get("rounds") or []

    league_type = safe_text(league_obj.get("type"))
    league_type_l = _norm_lower(league_type)

    has_standings = 1 if rows else 0
    has_groups = 1 if len(groups) > 1 else 0
    has_rounds = 1 if rounds else 0
    has_knockout_rounds = 1 if any(_infer_round_is_knockout(r) for r in rounds) else 0

    if league_type_l == "cup":
        if has_standings and has_knockout_rounds:
            format_hint = "league_phase_plus_knockout"
        elif has_standings:
            format_hint = "cup_with_standings"
        elif has_rounds:
            format_hint = "knockout_only"
        else:
            format_hint = "cup_other"
    else:
        if has_groups and has_knockout_rounds:
            format_hint = "multi_group_league_plus_playoff"
        elif has_groups:
            format_hint = "multi_group_league"
        elif has_knockout_rounds:
            format_hint = "single_table_league_plus_playoff"
        else:
            format_hint = "single_table_league"

    updated_utc = iso_utc(updated_at)

    execute(
        """
        INSERT INTO competition_season_meta (
            league_id, season,
            league_name, league_type, country_name,
            season_start, season_end, season_current,
            coverage_standings, coverage_events, coverage_lineups,
            coverage_statistics_fixtures, coverage_players,
            has_standings, standings_rows, has_groups, groups_count,
            has_rounds, rounds_count, has_knockout_rounds,
            format_hint, updated_utc
        )
        VALUES (
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (league_id, season) DO UPDATE SET
            league_name                  = EXCLUDED.league_name,
            league_type                  = EXCLUDED.league_type,
            country_name                 = EXCLUDED.country_name,
            season_start                 = EXCLUDED.season_start,
            season_end                   = EXCLUDED.season_end,
            season_current               = EXCLUDED.season_current,
            coverage_standings           = EXCLUDED.coverage_standings,
            coverage_events              = EXCLUDED.coverage_events,
            coverage_lineups             = EXCLUDED.coverage_lineups,
            coverage_statistics_fixtures = EXCLUDED.coverage_statistics_fixtures,
            coverage_players             = EXCLUDED.coverage_players,
            has_standings                = EXCLUDED.has_standings,
            standings_rows               = EXCLUDED.standings_rows,
            has_groups                   = EXCLUDED.has_groups,
            groups_count                 = EXCLUDED.groups_count,
            has_rounds                   = EXCLUDED.has_rounds,
            rounds_count                 = EXCLUDED.rounds_count,
            has_knockout_rounds          = EXCLUDED.has_knockout_rounds,
            format_hint                  = EXCLUDED.format_hint,
            updated_utc                  = EXCLUDED.updated_utc
        """,
        (
            int(league_id),
            int(season),
            safe_text(league_obj.get("name")),
            league_type,
            safe_text(country_obj.get("name")),
            safe_text(season_obj.get("start")),
            safe_text(season_obj.get("end")),
            _bool_to_int(season_obj.get("current")),
            _bool_to_int(cov.get("standings")),
            _bool_to_int(fixtures_cov.get("events")),
            _bool_to_int(fixtures_cov.get("lineups")),
            _bool_to_int(fixtures_cov.get("statistics_fixtures")),
            _bool_to_int(cov.get("players")),
            int(has_standings),
            int(len(rows)),
            int(has_groups),
            int(len(groups)),
            int(has_rounds),
            int(len(rounds)),
            int(has_knockout_rounds),
            format_hint,
            updated_utc,
        ),
    )

    return {
        "has_standings": has_standings,
        "groups_count": len(groups),
        "rounds_count": len(rounds),
        "has_knockout_rounds": has_knockout_rounds,
        "format_hint": format_hint,
    }


def sync_competition_reference(
    session: requests.Session,
    league_id: int,
    season: int,
    fetched_at: dt.datetime,
    *,
    league_meta_bundle: Optional[Dict[str, Any]] = None,
    standings_bundle: Optional[Dict[str, Any]] = None,
    rounds_bundle: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    league_meta_bundle = league_meta_bundle or fetch_league_season_meta(session, league_id, season)
    standings_bundle = standings_bundle or fetch_standings_bundle(session, league_id, season)
    rounds_bundle = rounds_bundle or fetch_rounds(session, league_id, season)

    upsert_competition_api_raw(league_id, season, "leagues", (league_meta_bundle or {}).get("raw") or {}, fetched_at)
    upsert_competition_api_raw(league_id, season, "standings", (standings_bundle or {}).get("raw") or {}, fetched_at)
    upsert_competition_api_raw(league_id, season, "fixtures_rounds", (rounds_bundle or {}).get("raw") or {}, fetched_at)

    groups = (standings_bundle or {}).get("groups") or []
    rounds = (rounds_bundle or {}).get("rounds") or []

    replace_rounds_for_league_season(league_id, season, rounds)
    replace_standings_group_meta(league_id, season, groups, fetched_at)
    replace_competition_rounds_meta(league_id, season, rounds, fetched_at)

    return upsert_competition_season_meta(
        league_id,
        season,
        league_meta_bundle,
        standings_bundle,
        rounds_bundle,
        fetched_at,
    )



def _now_iso_utc() -> str:
    return iso_utc(now_utc())


def enqueue_ft_trigger(fixture_id: int, league_id: int, season: int, finished_iso_utc: Optional[str] = None) -> None:
    """
    ✅ 어디서든 FINISHED 감지 시 호출해도 안전.
    - fixture_id PK로 중복 방지
    - finished_utc / created_utc는 최초 값 유지
    - updated_utc만 갱신
    """
    fin = finished_iso_utc or _now_iso_utc()
    nowi = _now_iso_utc()
    execute(
        """
        INSERT INTO ft_triggers (
            fixture_id, league_id, season,
            finished_utc,
            standings_consumed_utc,
            created_utc, updated_utc
        )
        VALUES (%s,%s,%s,%s,NULL,%s,%s)
        ON CONFLICT (fixture_id) DO UPDATE SET
            league_id    = EXCLUDED.league_id,
            season       = EXCLUDED.season,
            updated_utc  = EXCLUDED.updated_utc
        """,
        (int(fixture_id), int(league_id), int(season), fin, nowi, nowi),
    )


def cleanup_old_rows() -> None:
    """
    TTL(기본 90일) 정리.
    - text 컬럼을 timestamptz로 캐스팅해서 비교
    - 실패해도 워커 죽지 않게 best-effort
    """
    days = max(7, int(FT_TRIGGER_TTL_DAYS or 90))
    try:
        execute(
            """
            DELETE FROM ft_triggers
            WHERE NULLIF(created_utc,'')::timestamptz < (NOW() - (%s || ' days')::interval)
            """,
            (str(days),),
        )
    except Exception:
        pass


def _resolve_season_for_league_from_db(league_id: int) -> Optional[int]:
    """
    standings 워커/브라켓 워커에서 시즌 추정:
    - matches에서 MAX(season) 우선
    - fixtures에서 MAX(season) fallback
    """
    try:
        r = fetch_all(
            "SELECT MAX(season) AS s FROM matches WHERE league_id = %s",
            (int(league_id),),
        )
        if r:
            s = safe_int(r[0].get("s"))
            if s:
                return s
    except Exception:
        pass

    try:
        r = fetch_all(
            "SELECT MAX(season) AS s FROM fixtures WHERE league_id = %s",
            (int(league_id),),
        )
        if r:
            s = safe_int(r[0].get("s"))
            if s:
                return s
    except Exception:
        pass

    return None


def fetch_standings(session: requests.Session, league_id: int, season: int) -> List[Dict[str, Any]]:
    bundle = fetch_standings_bundle(session, league_id, season)
    return bundle.get("rows") or []


def upsert_standings_rows(league_id: int, season: int, rows: List[Dict[str, Any]]) -> int:
    """
    ✅ standings 테이블(네 스키마)에 맞춰 REPLACE (league_id+season 단위 싹 교체)

    - rows가 비면(응답 비정상/일시 빈값) 절대 삭제하지 않음
    - BEGIN/COMMIT으로 "삭제→삽입 사이 잠깐 빈 상태"를 최소화
    - PK: (league_id, season, group_name, team_id)
    """
    if not rows:
        return 0

    nowi = _now_iso_utc()

    # (선택) 매우 방어적으로, 같은 (group_name, team_id)가 중복으로 들어오면 마지막 값만 남기기
    # - API-Sports는 보통 중복을 안 주지만, 혹시 모를 공급자 이상치 방어
    dedup: Dict[Tuple[str, int], Dict[str, Any]] = {}

    for r in rows:
        team = r.get("team") or {}
        team_id = safe_int(team.get("id"))
        if team_id is None:
            continue

        group_name = (safe_text(r.get("group")) or "Overall").strip() or "Overall"
        dedup[(group_name, int(team_id))] = r

    if not dedup:
        return 0

    try:
        execute("BEGIN")
    except Exception:
        # BEGIN 실패해도 동작은 하게(최악: 잠깐 빈 상태 가능)
        pass

    # ✅ 핵심: league+season 전체 삭제
    execute(
        "DELETE FROM standings WHERE league_id=%s AND season=%s",
        (int(league_id), int(season)),
    )

    n = 0

    for (group_name, team_id), r in dedup.items():
        rank = safe_int(r.get("rank")) or 0
        points = safe_int(r.get("points"))
        goals_diff = safe_int(r.get("goalsDiff"))
        form = safe_text(r.get("form"))
        desc = safe_text(r.get("description"))
        update = safe_text(r.get("update")) or nowi

        all_blk = r.get("all") or {}
        played = safe_int(all_blk.get("played")) if isinstance(all_blk, dict) else None
        win = safe_int(all_blk.get("win")) if isinstance(all_blk, dict) else None
        draw = safe_int(all_blk.get("draw")) if isinstance(all_blk, dict) else None
        lose = safe_int(all_blk.get("lose")) if isinstance(all_blk, dict) else None

        gf = None
        ga = None
        goals_blk = (all_blk.get("goals") if isinstance(all_blk, dict) else None) or {}
        if isinstance(goals_blk, dict):
            gf = safe_int(goals_blk.get("for"))
            ga = safe_int(goals_blk.get("against"))

        # ✅ REPLACE에서는 ON CONFLICT가 사실상 불필요하지만,
        #    트랜잭션/중복 방어/재실행 안전성 때문에 "DO UPDATE"를 남겨둬도 무해함
        execute(
            """
            INSERT INTO standings (
                league_id, season, group_name, rank, team_id,
                points, goals_diff, played, win, draw, lose,
                goals_for, goals_against, form, updated_utc, description
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (league_id, season, group_name, team_id) DO UPDATE SET
                rank          = EXCLUDED.rank,
                points        = EXCLUDED.points,
                goals_diff    = EXCLUDED.goals_diff,
                played        = EXCLUDED.played,
                win           = EXCLUDED.win,
                draw          = EXCLUDED.draw,
                lose          = EXCLUDED.lose,
                goals_for     = EXCLUDED.goals_for,
                goals_against = EXCLUDED.goals_against,
                form          = EXCLUDED.form,
                updated_utc   = EXCLUDED.updated_utc,
                description   = EXCLUDED.description
            """,
            (
                int(league_id), int(season), group_name, int(rank), int(team_id),
                points, goals_diff, played, win, draw, lose,
                gf, ga, form, update, desc
            ),
        )
        n += 1

    try:
        execute("COMMIT")
    except Exception:
        try:
            execute("ROLLBACK")
        except Exception:
            pass
        raise

    return n



def _select_unconsumed_triggers(limit: int = 50) -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT fixture_id, league_id, season, finished_utc
        FROM ft_triggers
        WHERE standings_consumed_utc IS NULL
        ORDER BY NULLIF(finished_utc,'')::timestamptz ASC NULLS LAST, fixture_id ASC
        LIMIT %s
        """,
        (int(limit),),
    )
    return rows or []


def _mark_triggers_consumed(fixture_ids: List[int]) -> None:
    if not fixture_ids:
        return
    nowi = _now_iso_utc()
    execute(
        """
        UPDATE ft_triggers
        SET standings_consumed_utc = %s, updated_utc = %s
        WHERE fixture_id = ANY(%s)
          AND standings_consumed_utc IS NULL
        """,
        (nowi, nowi, fixture_ids),
    )



def run_once_standings(do_periodic: bool = True) -> int:
    """
    ✅ Standings 워커:
    - FT 트리거 즉시 1회(폴링): do_periodic=False 로 호출
    - 30분 주기 refresh: do_periodic=True 로 호출
    - 트리거 소비 방식: B(standings_consumed_utc)

    ✅ FIX(중요):
    - /standings 호출이 실패하거나 빈 응답이면 트리거를 소비하지 않는다.
      (그래야 다음 poll에서 재시도 가능)
    """
    if not API_KEY:
        print("[standings_worker] APIFOOTBALL_KEY(env) 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    league_ids = parse_live_leagues(LIVE_LEAGUES_ENV)
    if not league_ids:
        print("[standings_worker] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    if not hasattr(run_once_standings, "_ddl_done"):
        ensure_ft_triggers_table()
        ensure_competition_structure_tables()
        run_once_standings._ddl_done = True  # type: ignore[attr-defined]

    s = _session()

   
    triggers = _select_unconsumed_triggers(limit=60)

    # fixture_id -> (lid, season)
    trig_map: Dict[int, Tuple[int, int]] = {}
    processed_pairs: Set[Tuple[int, int]] = set()

    for t in triggers or []:
        fid = safe_int(t.get("fixture_id"))
        lid = safe_int(t.get("league_id"))
        season = safe_int(t.get("season"))
        if fid is None or lid is None or season is None:
            continue
        trig_map[int(fid)] = (int(lid), int(season))
        processed_pairs.add((int(lid), int(season)))

    total_rows = 0

    # ✅ 성공한 pair만 모아서 그 pair에 속한 trigger만 consumed 처리
    succeeded_pairs: Set[Tuple[int, int]] = set()

    for (lid, season) in sorted(processed_pairs):
        try:
            league_meta_bundle = fetch_league_season_meta(s, lid, season)
            standings_bundle = fetch_standings_bundle(s, lid, season)
            rounds_bundle = fetch_rounds(s, lid, season)

            rows = standings_bundle.get("rows") or []
            rounds_list = rounds_bundle.get("rounds") or []
            has_any_reference = bool((league_meta_bundle or {}).get("item")) or bool(rows) or bool(rounds_list)

            # ✅ standings가 비어도 leagues / rounds 원본이 있으면 성공으로 간주
            #    (컵대회, knockout-only 리그 대응)
            if not has_any_reference:
                print(f"[standings_worker] trigger_refresh league={lid} season={season} no_api_reference -> keep triggers (retry)")
                continue

            sync_info = sync_competition_reference(
                s,
                lid,
                season,
                now_utc(),
                league_meta_bundle=league_meta_bundle,
                standings_bundle=standings_bundle,
                rounds_bundle=rounds_bundle,
            )

            n = 0
            if rows:
                n = upsert_standings_rows(lid, season, rows)
                total_rows += n

            succeeded_pairs.add((lid, season))
            print(
                f"[standings_worker] trigger_refresh league={lid} season={season} "
                f"standings_rows={len(rows)} upserted={n} rounds={len(rounds_list)} "
                f"format={sync_info.get('format_hint')}"
            )

        except Exception as e:
            # ✅ 에러면 트리거 유지(재시도)
            print(f"[standings_worker] trigger_refresh league={lid} season={season} err: {e} -> keep triggers", file=sys.stderr)

    # ✅ 성공한 pair에 속한 fixture_id만 consumed 처리
    if triggers and succeeded_pairs:
        ok_fids: List[int] = []
        for fid, pair in trig_map.items():
            if pair in succeeded_pairs:
                ok_fids.append(int(fid))

        if ok_fids:
            try:
                _mark_triggers_consumed(ok_fids)
            except Exception:
                pass

    # ✅ 폴링 모드(do_periodic=False)면 여기서 종료
    if not do_periodic:
        try:
            cleanup_old_rows()
        except Exception:
            pass
        return total_rows

    # 2) 정기 refresh(30분): 시즌을 DB로 추정해서 1회 갱신
    # - 이미 트리거로 "성공 처리"한 (lid,season)은 중복 호출 피함
    for lid in league_ids:
        season = _resolve_season_for_league_from_db(lid)
        if season is None:
            continue
        if (lid, season) in succeeded_pairs:
            continue
        try:
            league_meta_bundle = fetch_league_season_meta(s, lid, season)
            standings_bundle = fetch_standings_bundle(s, lid, season)
            rounds_bundle = fetch_rounds(s, lid, season)

            sync_info = sync_competition_reference(
                s,
                lid,
                season,
                now_utc(),
                league_meta_bundle=league_meta_bundle,
                standings_bundle=standings_bundle,
                rounds_bundle=rounds_bundle,
            )

            rows = standings_bundle.get("rows") or []
            rounds_list = rounds_bundle.get("rounds") or []

            n = 0
            if rows:
                n = upsert_standings_rows(lid, season, rows)
                total_rows += n

            print(
                f"[standings_worker] periodic_refresh league={lid} season={season} "
                f"standings_rows={len(rows)} upserted={n} rounds={len(rounds_list)} "
                f"format={sync_info.get('format_hint')}"
            )
        except Exception as e:
            print(f"[standings_worker] periodic_refresh league={lid} season={season} err: {e}", file=sys.stderr)

    # 3) TTL 정리
    try:
        cleanup_old_rows()
    except Exception:
        pass

    return total_rows





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


def _read_postmatch_state(fixture_id: int) -> Optional[Dict[str, Any]]:
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

    ✅ 변경 정책:
    - events가 빈 배열([])이면 DB를 건드리지 않는다(삭제/삽입 모두 안 함).
      → 다음 틱에 데이터가 들어오면 그때 교체(insert)한다.
    - ✅ 레이스 차단: DB의 matches.status_group 이 아직 'INPLAY'면
      postmatch/backfill/watchdog 경로에서 match_events를 건드리지 않는다.
      (라이브중 이벤트는 live=all 경로가 전담)
    - ✅ DELETE + INSERT를 하나의 트랜잭션으로 묶는다.
    반환: insert된 row 수
    """
    cols = set(_get_table_columns("match_events"))
    if not cols:
        return 0

    if not events:
        return 0

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
    tx_started = False

    try:
        execute("BEGIN")
        tx_started = True

        execute("DELETE FROM match_events WHERE fixture_id = %s", (fixture_id,))

        for ev in events:
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

        execute("COMMIT")
        tx_started = False
        return inserted

    except Exception:
        if tx_started:
            try:
                execute("ROLLBACK")
            except Exception:
                pass
        raise





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

    ✅ 추가(복구용):
    - stats / lineups 가 DB에 비어있으면(또는 부족하면) postmatch 시점에 1회 보강
      * 호출 폭발 방지: +60s, +30m 실행 구간에서만 / 그리고 DB에 이미 있으면 스킵
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

    def _needs_stats(fid: int) -> bool:
        try:
            r = fetch_all(
                "SELECT 1 FROM match_team_stats WHERE fixture_id=%s LIMIT 1",
                (int(fid),),
            )
            return not bool(r)
        except Exception:
            # 조회 실패 시엔 과호출 방지 위해 "필요 없음" 취급
            return False

    def _needs_lineups(fid: int) -> bool:
        try:
            r = fetch_all(
                "SELECT COUNT(*) AS n FROM match_lineups WHERE fixture_id=%s",
                (int(fid),),
            )
            n = int((r[0].get("n") if r else 0) or 0)
            # 홈/원정 2팀 row가 있어야 정상
            return n < 2
        except Exception:
            return False

    def _try_fill_stats_and_lineups(tag: str) -> Tuple[bool, bool]:
        filled_stats = False
        filled_lineups = False

        # stats 보강
        if _needs_stats(fixture_id):
            try:
                stats = fetch_team_stats(session, fixture_id)
                if stats:
                    upsert_match_team_stats(fixture_id, stats)
                    filled_stats = True
            except Exception:
                pass

        # lineups 보강
        if _needs_lineups(fixture_id):
            try:
                lu = fetch_lineups(session, fixture_id)
                # upsert_match_lineups는 ready 기준이지만,
                # postmatch 복구 목적이므로 "DB에 들어갔는지"가 중요 → 함수 반환값은 참고만
                _ = upsert_match_lineups(fixture_id, lu, nowu)
                # 실제로 2팀이 채워졌는지 재확인
                if not _needs_lineups(fixture_id):
                    filled_lineups = True
            except Exception:
                pass

        if filled_stats or filled_lineups:
            print(f"      [postmatch_fill] fixture_id={fixture_id} tag={tag} stats={filled_stats} lineups={filled_lineups}")

        return filled_stats, filled_lineups

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

        # ✅ stats/lineups 보강(비어있으면)
        _try_fill_stats_and_lineups(tag="60s")

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

        # ✅ stats/lineups 보강(비어있으면)
        _try_fill_stats_and_lineups(tag="30m")

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
    """
    ✅ LIVE 전용(핫패스):
    - live=all 감지/즉시 처리
    - live_dates(오늘, 필요 시 어제) 스캔만 수행
    - backfill / watchdog 는 별도 워커(run_once_backfill / run_once_watchdog)가 담당
    """
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

        ensure_ft_triggers_table()
        ensure_competition_structure_tables()

        run_once._ddl_done = True  # type: ignore[attr-defined]


    now = now_utc()
    fetched_at = now
    s = _session()
    run_started_ts = time.time()

    total_fixtures = 0
    total_inplay = 0

    # ─────────────────────────────────────
    # (0) live=all 감지
    # ─────────────────────────────────────
    live_items: List[Dict[str, Any]] = []
    try:
        live_items = fetch_live_all(s)
    except Exception as e:
        print(f"[live_detect] err: {e}", file=sys.stderr)
        live_items = []

    watched = set(league_ids)

    # ✅ live=all에서 "현재 라이브가 있는 watched 리그"만 추출
    live_lids: Set[int] = set()
    for it in live_items or []:
        lg = it.get("league") or {}
        lid = safe_int(lg.get("id"))
        if lid is None:
            continue
        if lid in watched:
            live_lids.add(lid)

    idle_mode = (len(live_lids) == 0)
    if idle_mode:
        print(f"[live_detect] watched_live=0 (live_all={len(live_items)}) → all leagues slow scan mode")
    else:
        print(f"[live_detect] watched_live={len(live_lids)} (live_all={len(live_items)}) → mixed scan (live fast / non-live slow)")

    # ─────────────────────────────────────
    # (0-1) ✅ LIVE 아이템 즉시 처리
    # ─────────────────────────────────────
    for item in live_items or []:
        try:
            lg = item.get("league") or {}
            lid = safe_int(lg.get("id"))
            if lid is None or lid not in watched:
                continue

            fx = item.get("fixture") or {}
            fid = safe_int(fx.get("id"))
            if fid is None:
                continue

            season = safe_int(lg.get("season"))
            if season is None:
                season = safe_int((item.get("league") or {}).get("season"))

            if season is None:
                continue

            st = fx.get("status") or {}
            status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
            sg = map_status_group(status_short)

            upsert_fixture_row(
                fixture_id=fid,
                league_id=lid,
                season=season,
                date_utc=safe_text(fx.get("date")),
                status_short=status_short,
                status_group=sg,
            )

            fixture_id, home_id, away_id, sg2, date_utc = upsert_match_row_from_fixture(
                item, league_id=lid, season=season
            )

            try:
                upsert_match_fixtures_raw(fixture_id, item, fetched_at)
            except Exception:
                pass

            try:
                elapsed = safe_int((item.get("fixture") or {}).get("status", {}).get("elapsed"))
                maybe_sync_lineups(s, fixture_id, date_utc, sg2, elapsed, now)
            except Exception:
                pass

            try:
                maybe_sync_postmatch_timeline(s, fixture_id, sg2, now)
            except Exception:
                pass

            if sg2 == "FINISHED":
                try:
                    enqueue_ft_trigger(fixture_id, lid, season, finished_iso_utc=iso_utc(now))
                except Exception:
                    pass


            if sg2 == "INPLAY":
                total_inplay += 1

                try:
                    now_ts_events = time.time()
                    last_events_ts = LAST_EVENTS_SYNC.get(fixture_id)

                    if (last_events_ts is None) or ((now_ts_events - last_events_ts) >= EVENTS_INTERVAL_SEC):
                        events = fetch_events(s, fixture_id)

                        try:
                            upsert_match_events_raw(fixture_id, events, now)
                        except Exception:
                            pass

                        inserted = 0
                        try:
                            inserted = replace_match_events_for_fixture(fixture_id, events)
                        except Exception:
                            inserted = 0

                        try:
                            h_red, a_red = calc_red_cards_from_events(events, home_id, away_id)
                            upsert_match_live_state(fixture_id, h_red, a_red, now)
                        except Exception:
                            pass

                        LAST_EVENTS_SYNC[fixture_id] = now_ts_events
                        print(f"      [live_all_events] fixture_id={fixture_id} events={len(events)} inserted={inserted} interval={EVENTS_INTERVAL_SEC}s")
                    else:
                        remain = EVENTS_INTERVAL_SEC - (now_ts_events - last_events_ts)
                        print(f"      [live_events_skip] fixture_id={fixture_id} remain={remain:.1f}s")
                except Exception as e:
                    print(f"      [live_all_events] fixture_id={fixture_id} err: {e}", file=sys.stderr)

                try:
                    now_ts3 = time.time()
                    last_ts = LAST_STATS_SYNC.get(fixture_id)
                    if (last_ts is None) or ((now_ts3 - last_ts) >= STATS_INTERVAL_SEC):
                        stats = fetch_team_stats(s, fixture_id)
                        upsert_match_team_stats(fixture_id, stats)
                        LAST_STATS_SYNC[fixture_id] = now_ts3
                        print(f"      [live_all_stats] fixture_id={fixture_id} updated")
                except Exception as e:
                    print(f"      [live_all_stats] fixture_id={fixture_id} err: {e}", file=sys.stderr)

        except Exception as e:
            print(f"  ! live_all item 처리 중 에러: {e}", file=sys.stderr)

    # ─────────────────────────────────────
    # (1) league/date 시즌 & 무경기 캐시 (API 낭비 감소)
    # ─────────────────────────────────────
    if not hasattr(run_once, "_fixtures_cache"):
        run_once._fixtures_cache = {}  # type: ignore[attr-defined]
    fc: Dict[Tuple[int, str], Dict[str, Any]] = run_once._fixtures_cache  # type: ignore[attr-defined]

    SEASON_TTL = 60 * 60
    NOFIX_TTL = 60 * 10

    now_ts = time.time()
    for k, v in list(fc.items()):
        if float(v.get("exp") or 0) < now_ts:
            del fc[k]

    fast_leagues = set(parse_fast_leagues(FAST_LEAGUES_ENV))
    fixture_groups: Dict[int, str] = {}

    live_dates = target_dates_for_live()

    def _pick_scan_interval(lid: int) -> int:
        if lid in fast_leagues:
            return int(DETECT_INTERVAL_SEC)
        if lid in live_lids:
            return int(DEFAULT_SCAN_INTERVAL_SEC)
        return int(IDLE_SCAN_INTERVAL_SEC)

    def _scan_fixtures_for_dates(date_list: List[str], mode_tag: str, forced_interval: Optional[int] = None) -> None:
        nonlocal total_fixtures, fixture_groups

        combos: List[Tuple[str, int]] = []
        for date_str in date_list:
            for lid in league_ids:
                combos.append((date_str, lid))
        if not combos:
            return

        for date_str, lid in combos:
            scan_interval = int(forced_interval) if forced_interval is not None else _pick_scan_interval(lid)

            k_scan = (lid, f"{date_str}|{mode_tag}")
            last_scan = float(LAST_FIXTURES_SCAN_TS.get(k_scan) or 0.0)
            if scan_interval > 0 and (now_ts - last_scan) < scan_interval:
                continue
            LAST_FIXTURES_SCAN_TS[k_scan] = now_ts

            fixtures: List[Dict[str, Any]] = []
            used_season: Optional[int] = None

            cache_key = (lid, date_str)
            cached = fc.get(cache_key)

            if cached and float(cached.get("exp") or 0) >= now_ts:
                if cached.get("no") is True:
                    continue

                cached_season = cached.get("season")
                if isinstance(cached_season, int):
                    try:
                        rows = fetch_fixtures(s, lid, date_str, cached_season)
                        if rows:
                            fixtures = rows
                            used_season = cached_season
                        else:
                            fc.pop(cache_key, None)
                    except Exception as e:
                        fc.pop(cache_key, None)
                        print(f"  [fixtures] league={lid} date={date_str} season={cached_season} err: {e}", file=sys.stderr)

            if used_season is None:
                for season in infer_season_candidates(date_str):
                    try:
                        rows = fetch_fixtures(s, lid, date_str, season)
                        if rows:
                            fixtures = rows
                            used_season = season
                            fc[cache_key] = {"season": season, "no": False, "exp": now_ts + SEASON_TTL}
                            break
                    except Exception as e:
                        print(f"  [fixtures] league={lid} date={date_str} season={season} err: {e}", file=sys.stderr)

            if used_season is None:
                fc[cache_key] = {"season": None, "no": True, "exp": now_ts + NOFIX_TTL}
                continue

            total_fixtures += len(fixtures)
            print(f"[fixtures:{mode_tag}] league={lid} date={date_str} season={used_season} count={len(fixtures)} interval={scan_interval}s")

            for item in fixtures:
                try:
                    fx = item.get("fixture") or {}
                    fid = safe_int(fx.get("id"))
                    if fid is None:
                        continue

                    st = fx.get("status") or {}
                    status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
                    status_group = map_status_group(status_short)

                    # ✅ INPLAY(라이브중)은 live=all이 전담 → 여기서는 스킵
                    if status_group == "INPLAY":
                        continue

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

                    try:
                        maybe_sync_postmatch_timeline(s, fixture_id, sg, now)
                    except Exception as pm_err:
                        print(f"      [postmatch_timeline] fixture_id={fixture_id} err: {pm_err}", file=sys.stderr)

                    # ✅ FT 트리거 기록(B안)
                    if sg == "FINISHED":
                        try:
                            enqueue_ft_trigger(fixture_id, lid, used_season, finished_iso_utc=iso_utc(now))
                        except Exception:
                            pass


                except Exception as e:
                    print(f"  ! fixture 처리 중 에러: {e}", file=sys.stderr)

    # ✅ LIVE 스캔만 수행 (backfill은 별도 워커로 분리)
    #    단, 라이브 핫패스가 이미 오래 걸렸으면 이번 틱의 date scan은 건너뛴다.
    hotpath_elapsed = time.time() - run_started_ts
    if (total_inplay > 0) and (hotpath_elapsed >= LIVE_HOTPATH_BUDGET_SEC):
        print(f"[live_scan_skip] hotpath_elapsed={hotpath_elapsed:.2f}s inplay={total_inplay} budget={LIVE_HOTPATH_BUDGET_SEC}s")
    else:
        _scan_fixtures_for_dates(live_dates, mode_tag="live", forced_interval=None)

    # ─────────────────────────────────────
    # (6) 런타임 캐시 prune (메모리 누적 방지)
    # ─────────────────────────────────────
    try:
        for fid, g in list(fixture_groups.items()):
            if g in ("FINISHED", "OTHER"):
                LAST_STATS_SYNC.pop(fid, None)
                LAST_EVENTS_SYNC.pop(fid, None)
                LINEUPS_STATE.pop(fid, None)

        if len(LINEUPS_STATE) > 3000:
            for fid in list(LINEUPS_STATE.keys())[: len(LINEUPS_STATE) - 2000]:
                LINEUPS_STATE.pop(fid, None)
    except Exception:
        pass

    run_sec = time.time() - run_started_ts
    print(f"[live_status_worker] done. total_fixtures={total_fixtures}, inplay={total_inplay}, run_sec={run_sec:.2f}")
    return total_inplay



def run_once_backfill() -> int:
    """
    ✅ BACKFILL 전용(슬로우패스):
    - 어제/오늘/내일 backfill_dates 스캔만 수행
    - INPLAY는 스킵(이미 scan 로직에서 status_group==INPLAY continue)
    """
    if not API_KEY:
        print("[backfill_worker] APIFOOTBALL_KEY(env) 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    league_ids = parse_live_leagues(LIVE_LEAGUES_ENV)
    if not league_ids:
        print("[backfill_worker] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    # ✅ DDL은 워커 시작 시 1회만(FT postmatch state / red state 공용)
    if not hasattr(run_once_backfill, "_ddl_done"):
        ensure_match_live_state_table()
        ensure_match_postmatch_timeline_state_table()

        ensure_ft_triggers_table()
        ensure_competition_structure_tables()

        run_once_backfill._ddl_done = True  # type: ignore[attr-defined]


    now = now_utc()
    fetched_at = now
    s = _session()

    total_fixtures = 0

    # fixtures cache는 live와 공유하지 않아도 됨(프로세스가 분리되면 자연 분리)
    if not hasattr(run_once_backfill, "_fixtures_cache"):
        run_once_backfill._fixtures_cache = {}  # type: ignore[attr-defined]
    fc: Dict[Tuple[int, str], Dict[str, Any]] = run_once_backfill._fixtures_cache  # type: ignore[attr-defined]

    SEASON_TTL = 60 * 60
    NOFIX_TTL = 60 * 10

    now_ts = time.time()
    for k, v in list(fc.items()):
        if float(v.get("exp") or 0) < now_ts:
            del fc[k]

    backfill_dates = target_dates_for_scan()

    # backfill은 “주기 고정”이므로 forced_interval로 DATE_SCAN_INTERVAL_SEC를 사용
    forced_interval = int(DATE_SCAN_INTERVAL_SEC)

    # (date,lid) 조합 만들기
    combos: List[Tuple[str, int]] = []
    for date_str in backfill_dates:
        for lid in league_ids:
            combos.append((date_str, lid))

    if not combos:
        return 0

    for date_str, lid in combos:
        k_scan = (lid, f"{date_str}|backfill")
        last_scan = float(LAST_FIXTURES_SCAN_TS.get(k_scan) or 0.0)
        if forced_interval > 0 and (now_ts - last_scan) < forced_interval:
            continue
        LAST_FIXTURES_SCAN_TS[k_scan] = now_ts

        fixtures: List[Dict[str, Any]] = []
        used_season: Optional[int] = None

        cache_key = (lid, date_str)
        cached = fc.get(cache_key)

        if cached and float(cached.get("exp") or 0) >= now_ts:
            if cached.get("no") is True:
                continue

            cached_season = cached.get("season")
            if isinstance(cached_season, int):
                try:
                    rows = fetch_fixtures(s, lid, date_str, cached_season)
                    if rows:
                        fixtures = rows
                        used_season = cached_season
                    else:
                        fc.pop(cache_key, None)
                except Exception as e:
                    fc.pop(cache_key, None)
                    print(f"  [backfill:fixtures] league={lid} date={date_str} season={cached_season} err: {e}", file=sys.stderr)

        if used_season is None:
            for season in infer_season_candidates(date_str):
                try:
                    rows = fetch_fixtures(s, lid, date_str, season)
                    if rows:
                        fixtures = rows
                        used_season = season
                        fc[cache_key] = {"season": season, "no": False, "exp": now_ts + SEASON_TTL}
                        break
                except Exception as e:
                    print(f"  [backfill:fixtures] league={lid} date={date_str} season={season} err: {e}", file=sys.stderr)

        if used_season is None:
            fc[cache_key] = {"season": None, "no": True, "exp": now_ts + NOFIX_TTL}
            continue

        total_fixtures += len(fixtures)
        print(f"[fixtures:backfill] league={lid} date={date_str} season={used_season} count={len(fixtures)} interval={forced_interval}s")

        for item in fixtures:
            try:
                fx = item.get("fixture") or {}
                fid = safe_int(fx.get("id"))
                if fid is None:
                    continue

                st = fx.get("status") or {}
                status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
                status_group = map_status_group(status_short)

                # ✅ INPLAY는 backfill이 절대 건드리지 않음
                if status_group == "INPLAY":
                    continue

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
                except Exception:
                    pass

                # 라인업 슬롯/정책은 backfill에서도 “프리매치/초반” 케이스에 도움 됨(그대로 유지)
                try:
                    elapsed = safe_int((item.get("fixture") or {}).get("status", {}).get("elapsed"))
                    maybe_sync_lineups(s, fixture_id, date_utc, sg, elapsed, now)
                except Exception:
                    pass

                # FT 이후 2회 덮어쓰기 정책 유지
                try:
                    maybe_sync_postmatch_timeline(s, fixture_id, sg, now)
                except Exception:
                    pass

                # ✅ FT 트리거 기록(B안)
                if sg == "FINISHED":
                    try:
                        enqueue_ft_trigger(fixture_id, lid, used_season, finished_iso_utc=iso_utc(now))
                    except Exception:
                        pass


            except Exception as e:
                print(f"  ! backfill fixture 처리 중 에러: {e}", file=sys.stderr)

    # ✅ 추가: DB에 저장된 예정 경기 전체를 순환 재검증
    # - date 기반 스캔으로는 "멀리 미래로 밀린 경기"를 다시 못 잡을 수 있으므로
    # - fixture_id 단건조회로 NS/TBD/PST/UPCOMING 경기의 최신 일정/상태를 반영
    try:
        rechecked = recheck_scheduled_fixtures(
            s,
            fetched_at,
            limit=SCHEDULE_RECHECK_LIMIT,
        )
    except Exception as e:
        rechecked = 0
        print(f"[backfill_worker] schedule_recheck err: {e}", file=sys.stderr)

    print(f"[backfill_worker] done. total_fixtures={total_fixtures}, rechecked={rechecked}")
    return total_fixtures


def run_once_watchdog() -> int:
    """
    ✅ WATCHDOG 전용:
    - DB에 오래 남은 INPLAY 후보를 뽑아서 단건 /fixtures?id= 로 상태 보정
    - WATCHDOG_INTERVAL_SEC 주기는 watchdog_fix_stale_inplay 내부에서도 한 번 더 막음
    """
    if not API_KEY:
        print("[watchdog_worker] APIFOOTBALL_KEY(env) 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    league_ids = parse_live_leagues(LIVE_LEAGUES_ENV)
    if not league_ids:
        print("[watchdog_worker] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    if not hasattr(run_once_watchdog, "_ddl_done"):
        ensure_match_live_state_table()
        ensure_match_postmatch_timeline_state_table()

        ensure_ft_triggers_table()
        ensure_competition_structure_tables()

        run_once_watchdog._ddl_done = True  # type: ignore[attr-defined]


    now = now_utc()
    s = _session()

    try:
        tried = watchdog_fix_stale_inplay(s, now)
        print(f"[watchdog_worker] done. tried={tried}")
        return tried
    except Exception as e:
        print(f"[watchdog_worker] err: {e}", file=sys.stderr)
        return 0






# ─────────────────────────────────────
# 루프
# ─────────────────────────────────────

# 역할 분기(파일 1개로 워커 3개 실행할 때 사용)
LIVE_WORKER_ROLE = (os.environ.get("LIVE_WORKER_ROLE") or "live").strip().lower()
# live | backfill | watchdog

def loop() -> None:
    """
    ✅ 단일 파일에서 역할별로 루프를 분기한다.

    핵심 수정:
    - standings는 TRIGGER_POLL_SEC마다 "트리거만" 처리(do_periodic=False)
    - STANDINGS_LOOP_SEC마다 "정기 작업" 1회(do_periodic=True)
    - 이렇게 해야 호출수 폭발 없이 'FT 즉시 1회 + 주기적 갱신'이 정확히 성립함
    """
    role = LIVE_WORKER_ROLE

    if role == "backfill":
        sleep_sec = int(os.environ.get("LIVE_BACKFILL_LOOP_SEC", str(DATE_SCAN_INTERVAL_SEC)))
        print(f"[live_status_worker] start role=backfill (loop_sec={sleep_sec}s)")
        while True:
            try:
                run_once_backfill()
            except Exception:
                traceback.print_exc()
            time.sleep(max(30, sleep_sec))

    elif role == "watchdog":
        sleep_sec = int(os.environ.get("LIVE_WATCHDOG_LOOP_SEC", str(WATCHDOG_INTERVAL_SEC)))
        print(f"[live_status_worker] start role=watchdog (loop_sec={sleep_sec}s)")
        while True:
            try:
                run_once_watchdog()
            except Exception:
                traceback.print_exc()
            time.sleep(max(10, sleep_sec))

    elif role == "standings":
        print(f"[live_status_worker] start role=standings (periodic={STANDINGS_LOOP_SEC}s, poll={TRIGGER_POLL_SEC}s)")
        last_periodic = 0.0
        while True:
            try:
                now_ts = time.time()

                # 1) 트리거 폴링: 트리거만 처리 (즉시성)
                run_once_standings(do_periodic=False)

                # 2) 주기적 전체 갱신: 30분에 1번만
                if (now_ts - last_periodic) >= float(STANDINGS_LOOP_SEC):
                    last_periodic = now_ts
                    run_once_standings(do_periodic=True)

            except Exception:
                traceback.print_exc()

            time.sleep(max(5, int(TRIGGER_POLL_SEC)))


    else:
        # default = live
        print(
            f"[live_status_worker] start role=live (detect_interval={DETECT_INTERVAL_SEC}s, default_scan={DEFAULT_SCAN_INTERVAL_SEC}s, fast_leagues_env='{FAST_LEAGUES_ENV}')"
        )
        while True:
            try:
                run_once()
            except Exception:
                traceback.print_exc()
            time.sleep(DETECT_INTERVAL_SEC)




if __name__ == "__main__":
    loop()


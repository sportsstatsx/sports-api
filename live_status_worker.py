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
from typing import Any, Dict, List, Optional, Tuple, Set

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

# ✅ (D) date(league/date/season) 스캔은 "어제/오늘/내일"만 1시간에 1번(백필/FT 보정용)
DATE_SCAN_INTERVAL_SEC = int(os.environ.get("LIVE_DATE_SCAN_INTERVAL_SEC", "3600"))

# ✅ (D-2) backfill B안: run_once에서 backfill이 live 틱을 밀지 않게 "처리 예산" 제한
BACKFILL_BUDGET_SEC = float(os.environ.get("LIVE_BACKFILL_BUDGET_SEC", "2.5"))   # run_once당 backfill에 쓸 최대 시간
BACKFILL_MAX_COMBOS = int(os.environ.get("LIVE_BACKFILL_MAX_COMBOS", "8"))       # run_once당 (date,lid) 최대 처리 개수


# ✅ (E) 워치독: DB에 오래 남은 INPLAY를 60초마다 단건 조회로 FT 보정
WATCHDOG_INTERVAL_SEC = int(os.environ.get("LIVE_WATCHDOG_INTERVAL_SEC", "60"))
WATCHDOG_STALE_HOURS = float(os.environ.get("LIVE_WATCHDOG_STALE_HOURS", "2"))
WATCHDOG_LIMIT = int(os.environ.get("LIVE_WATCHDOG_LIMIT", "50"))


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

# ✅ 워치독 실행 타이밍
LAST_WATCHDOG_TS: float = 0.0

# ✅ backfill B안: (date,lid) 조합을 run_once마다 분할 처리하기 위한 커서
BACKFILL_CURSOR: int = 0







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
BRACKET_LOOP_SEC   = int(os.environ.get("BRACKET_LOOP_SEC", "3600"))    # 60분

TRIGGER_POLL_SEC   = int(os.environ.get("FT_TRIGGER_POLL_SEC", "10"))   # standings/bracket에서 트리거 폴링 간격(짧게)


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
    # 조회/정리 성능을 위한 인덱스(있으면 좋고 없어도 동작은 동일)
    execute("CREATE INDEX IF NOT EXISTS idx_ft_triggers_created_utc ON ft_triggers (created_utc)")
    execute("CREATE INDEX IF NOT EXISTS idx_ft_triggers_league_season ON ft_triggers (league_id, season)")


def ensure_tournament_ties_table() -> None:
    """
    브라켓/플레이오프(녹아웃) 표시용 '가공 결과' 테이블
    - 앱에서 그리기 쉬운 형태로 저장(B안)
    - raw를 다 때려박는 게 아니라, tie 단위로 leg/agg/winner를 서버에서 정리
    """
    execute(
        """
        CREATE TABLE IF NOT EXISTS tournament_ties (
            league_id        integer NOT NULL,
            season           integer NOT NULL,
            round_name       text    NOT NULL,
            tie_key          text    NOT NULL,

            team_a_id        integer,
            team_b_id        integer,

            leg1_fixture_id  integer,
            leg2_fixture_id  integer,

            leg1_home_id     integer,
            leg1_away_id     integer,
            leg1_home_ft     integer,
            leg1_away_ft     integer,
            leg1_date_utc    text,

            leg2_home_id     integer,
            leg2_away_id     integer,
            leg2_home_ft     integer,
            leg2_away_ft     integer,
            leg2_date_utc    text,

            agg_a            integer,
            agg_b            integer,
            winner_team_id   integer,

            updated_utc      text,

            PRIMARY KEY (league_id, season, round_name, tie_key)
        )
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_tournament_ties_round ON tournament_ties (league_id, season, round_name)")


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

    try:
        execute(
            """
            DELETE FROM tournament_ties
            WHERE NULLIF(updated_utc,'')::timestamptz < (NOW() - (%s || ' days')::interval)
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
    data = api_get(session, "/standings", {"league": league_id, "season": season})
    resp = (data.get("response") or []) if isinstance(data, dict) else []
    if not resp:
        return []
    league = (resp[0].get("league") or {}) if isinstance(resp[0], dict) else {}
    standings = league.get("standings")
    if not isinstance(standings, list):
        return []

    # API-Sports는 [ [table...] ] 형태가 많음
    if standings and isinstance(standings[0], list):
        out: List[Dict[str, Any]] = []
        for tbl in standings:
            if isinstance(tbl, list):
                out.extend([x for x in tbl if isinstance(x, dict)])
        return out

    # 혹시 flat list면 그대로
    return [x for x in standings if isinstance(x, dict)]


def upsert_standings_rows(league_id: int, season: int, rows: List[Dict[str, Any]]) -> int:
    """
    standings 테이블(네 스키마)에 맞춰 UPSERT
    PK: (league_id, season, group_name, team_id)
    """
    if not rows:
        return 0

    nowi = _now_iso_utc()
    n = 0

    for r in rows:
        team = r.get("team") or {}
        team_id = safe_int(team.get("id"))
        if team_id is None:
            continue

        rank = safe_int(r.get("rank")) or 0
        points = safe_int(r.get("points"))
        goals_diff = safe_int(r.get("goalsDiff"))
        group_name = (safe_text(r.get("group")) or "Overall").strip() or "Overall"
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
            WHERE
                standings.rank          IS DISTINCT FROM EXCLUDED.rank OR
                standings.points        IS DISTINCT FROM EXCLUDED.points OR
                standings.goals_diff    IS DISTINCT FROM EXCLUDED.goals_diff OR
                standings.played        IS DISTINCT FROM EXCLUDED.played OR
                standings.win           IS DISTINCT FROM EXCLUDED.win OR
                standings.draw          IS DISTINCT FROM EXCLUDED.draw OR
                standings.lose          IS DISTINCT FROM EXCLUDED.lose OR
                standings.goals_for     IS DISTINCT FROM EXCLUDED.goals_for OR
                standings.goals_against IS DISTINCT FROM EXCLUDED.goals_against OR
                standings.form          IS DISTINCT FROM EXCLUDED.form OR
                standings.updated_utc   IS DISTINCT FROM EXCLUDED.updated_utc OR
                standings.description   IS DISTINCT FROM EXCLUDED.description
            """,
            (
                int(league_id), int(season), group_name, int(rank), int(team_id),
                points, goals_diff, played, win, draw, lose,
                gf, ga, form, update, desc
            ),
        )
        n += 1

    return n


def _select_unconsumed_triggers(which: str, limit: int = 50) -> List[Dict[str, Any]]:
    col = "standings_consumed_utc" if which == "standings" else "bracket_consumed_utc"
    rows = fetch_all(
        f"""
        SELECT fixture_id, league_id, season, finished_utc
        FROM ft_triggers
        WHERE {col} IS NULL
        ORDER BY NULLIF(finished_utc,'')::timestamptz ASC NULLS LAST, fixture_id ASC
        LIMIT %s
        """,
        (int(limit),),
    )
    return rows or []


def _mark_triggers_consumed(which: str, fixture_ids: List[int]) -> None:
    if not fixture_ids:
        return
    col = "standings_consumed_utc" if which == "standings" else "bracket_consumed_utc"
    nowi = _now_iso_utc()
    # IN (...) 안전하게 array로 처리
    execute(
        f"""
        UPDATE ft_triggers
        SET {col} = %s, updated_utc = %s
        WHERE fixture_id = ANY(%s)
          AND {col} IS NULL
        """,
        (nowi, nowi, fixture_ids),
    )


def _bracket_round_names() -> Set[str]:
    # 너 DB에서 실제로 보이는 round 값(Quarter-finals/Semi-finals/Final)을 기본 포함
    # 다른 대륙컵/리그도 방어적으로 몇 개 더 포함(없으면 그냥 빈 결과)
    return {
        "Final",
        "Semi-finals",
        "Quarter-finals",
        "Round of 16",
        "Round of 32",
        "Round of 64",
        "Play-offs",
        "Playoff",
        "Play-off",
        "1st Round",
        "2nd Round",
        "3rd Round",
    }


def build_and_upsert_bracket_for_league_season(league_id: int, season: int) -> int:
    """
    DB의 matches(이미 저장된 경기)에서 round 기반으로 tie를 만들고 tournament_ties에 upsert.
    - 두 팀 조합(순서 무관) + round_name 기준으로 tie_key 생성
    - 2leg면 date_utc로 leg1/leg2 정렬
    """
    rounds = _bracket_round_names()
    # DB에 실제로 존재하는 round만 가져오도록 ILIKE로 완화하지 않고 정확 매칭(너 데이터가 깔끔함)
    rows = fetch_all(
        """
        SELECT fixture_id, league_round, date_utc,
               home_id, away_id, home_ft, away_ft,
               status_group
        FROM matches
        WHERE league_id = %s
          AND season = %s
          AND league_round IS NOT NULL
          AND league_round <> ''
          AND status_group = 'FINISHED'
        """,
        (int(league_id), int(season)),
    ) or []

    # round 필터
    filtered: List[Dict[str, Any]] = []
    for r in rows:
        rn = (safe_text(r.get("league_round")) or "").strip()
        if not rn:
            continue
        if rn in rounds:
            filtered.append(r)

    if not filtered:
        return 0

    # group by (round_name, pair_key)
    def _pair_key(h: int, a: int) -> Tuple[int, int]:
        return (h, a) if h <= a else (a, h)

    def _parse_dt(s: Optional[str]) -> float:
        if not s:
            return 0.0
        try:
            x = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
            if x.tzinfo is None:
                x = x.replace(tzinfo=dt.timezone.utc)
            return x.timestamp()
        except Exception:
            return 0.0

    buckets: Dict[Tuple[str, Tuple[int, int]], List[Dict[str, Any]]] = {}
    for r in filtered:
        hid = safe_int(r.get("home_id")) or 0
        aid = safe_int(r.get("away_id")) or 0
        if hid == 0 or aid == 0:
            continue
        rn = (safe_text(r.get("league_round")) or "").strip()
        pk = _pair_key(hid, aid)
        buckets.setdefault((rn, pk), []).append(r)

    nowi = _now_iso_utc()
    upserted = 0

    for (round_name, (ta, tb)), games in buckets.items():
        games.sort(key=lambda x: _parse_dt(safe_text(x.get("date_utc"))))

        leg1 = games[0]
        leg2 = games[1] if len(games) >= 2 else None

        # agg 계산: team_a_id=ta, team_b_id=tb 기준(홈/원정 상관없이 합)
        def _score_for(team_id: int, g: Dict[str, Any]) -> int:
            hid = safe_int(g.get("home_id")) or 0
            aid = safe_int(g.get("away_id")) or 0
            hft = safe_int(g.get("home_ft"))
            aft = safe_int(g.get("away_ft"))
            if hft is None or aft is None:
                return 0
            if team_id == hid:
                return int(hft)
            if team_id == aid:
                return int(aft)
            return 0

        agg_a = _score_for(ta, leg1) + (_score_for(ta, leg2) if leg2 else 0)
        agg_b = _score_for(tb, leg1) + (_score_for(tb, leg2) if leg2 else 0)

        winner = None
        if leg2 is None:
            # 단판: leg1 결과로 승자
            if agg_a > agg_b:
                winner = ta
            elif agg_b > agg_a:
                winner = tb
        else:
            # 2leg: aggregate로 승자(동률은 winner 미결정으로 둠)
            if agg_a > agg_b:
                winner = ta
            elif agg_b > agg_a:
                winner = tb

        tie_key = f"{ta}-{tb}"  # round_name과 함께 PK 구성됨

        execute(
            """
            INSERT INTO tournament_ties (
                league_id, season, round_name, tie_key,
                team_a_id, team_b_id,
                leg1_fixture_id, leg2_fixture_id,
                leg1_home_id, leg1_away_id, leg1_home_ft, leg1_away_ft, leg1_date_utc,
                leg2_home_id, leg2_away_id, leg2_home_ft, leg2_away_ft, leg2_date_utc,
                agg_a, agg_b, winner_team_id,
                updated_utc
            )
            VALUES (
                %s,%s,%s,%s,
                %s,%s,
                %s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s
            )
            ON CONFLICT (league_id, season, round_name, tie_key) DO UPDATE SET
                leg1_fixture_id  = EXCLUDED.leg1_fixture_id,
                leg2_fixture_id  = EXCLUDED.leg2_fixture_id,
                leg1_home_id     = EXCLUDED.leg1_home_id,
                leg1_away_id     = EXCLUDED.leg1_away_id,
                leg1_home_ft     = EXCLUDED.leg1_home_ft,
                leg1_away_ft     = EXCLUDED.leg1_away_ft,
                leg1_date_utc    = EXCLUDED.leg1_date_utc,
                leg2_home_id     = EXCLUDED.leg2_home_id,
                leg2_away_id     = EXCLUDED.leg2_away_id,
                leg2_home_ft     = EXCLUDED.leg2_home_ft,
                leg2_away_ft     = EXCLUDED.leg2_away_ft,
                leg2_date_utc    = EXCLUDED.leg2_date_utc,
                agg_a            = EXCLUDED.agg_a,
                agg_b            = EXCLUDED.agg_b,
                winner_team_id   = EXCLUDED.winner_team_id,
                updated_utc      = EXCLUDED.updated_utc
            """,
            (
                int(league_id), int(season), round_name, tie_key,
                int(ta), int(tb),
                int(leg1.get("fixture_id")), (int(leg2.get("fixture_id")) if leg2 else None),
                safe_int(leg1.get("home_id")), safe_int(leg1.get("away_id")),
                safe_int(leg1.get("home_ft")), safe_int(leg1.get("away_ft")),
                safe_text(leg1.get("date_utc")),
                (safe_int(leg2.get("home_id")) if leg2 else None),
                (safe_int(leg2.get("away_id")) if leg2 else None),
                (safe_int(leg2.get("home_ft")) if leg2 else None),
                (safe_int(leg2.get("away_ft")) if leg2 else None),
                (safe_text(leg2.get("date_utc")) if leg2 else None),
                int(agg_a), int(agg_b), (int(winner) if winner else None),
                nowi,
            ),
        )
        upserted += 1

    return upserted


def bracket_fill_missing_rounds(session: requests.Session, league_id: int, season: int, limit: int = 50) -> int:
    """
    ✅ /fixtures로 league.round / raw 채우기 담당(우리가 합의한 'round/raw 채우기')
    - DB matches에서 league_round 빈 finished 경기들을 일부(limit) 골라서
      /fixtures?id= 로 단건 보충 → upsert_match_row_from_fixture가 league_round 채움
      + match_fixtures_raw 저장(best-effort)
    호출수 폭발 방지: LIMIT
    """
    rows = fetch_all(
        """
        SELECT fixture_id
        FROM matches
        WHERE league_id = %s
          AND season = %s
          AND status_group = 'FINISHED'
          AND (league_round IS NULL OR league_round = '')
        ORDER BY NULLIF(date_utc,'')::timestamptz ASC NULLS LAST
        LIMIT %s
        """,
        (int(league_id), int(season), int(limit)),
    ) or []

    if not rows:
        return 0

    done = 0
    nowu = now_utc()

    for r in rows:
        fid = safe_int(r.get("fixture_id"))
        if fid is None:
            continue
        try:
            fx_obj = fetch_fixture_by_id(session, fid)
            if not fx_obj:
                continue

            # upsert_match_row_from_fixture 안에서 league_round 채움
            upsert_match_row_from_fixture(fx_obj, league_id=int(league_id), season=int(season))

            try:
                upsert_match_fixtures_raw(fid, fx_obj, nowu)
            except Exception:
                pass

            done += 1
        except Exception:
            pass

    return done


def run_once_standings() -> int:
    """
    ✅ Standings 워커:
    - 30분 주기 refresh + FT 트리거 즉시 1회
    - 트리거 소비 방식: B(standings_consumed_utc)
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
        run_once_standings._ddl_done = True  # type: ignore[attr-defined]

    nowu = now_utc()
    s = _session()

    # 1) 트리거 우선 처리(미소비)
    triggers = _select_unconsumed_triggers("standings", limit=60)
    processed_pairs: Set[Tuple[int, int]] = set()
    for t in triggers:
        lid = safe_int(t.get("league_id"))
        season = safe_int(t.get("season"))
        if lid is None or season is None:
            continue
        processed_pairs.add((lid, season))

    # 트리거로 묶인 (lid,season)만 우선 갱신
    total_rows = 0
    for (lid, season) in sorted(processed_pairs):
        try:
            rows = fetch_standings(s, lid, season)
            n = upsert_standings_rows(lid, season, rows)
            total_rows += n
            print(f"[standings_worker] trigger_refresh league={lid} season={season} rows={len(rows)} upserted={n}")
        except Exception as e:
            print(f"[standings_worker] trigger_refresh league={lid} season={season} err: {e}", file=sys.stderr)

    # 트리거 소비(개별 fixture 단위)
    if triggers:
        fids = [int(x.get("fixture_id")) for x in triggers if safe_int(x.get("fixture_id")) is not None]
        try:
            _mark_triggers_consumed("standings", fids)
        except Exception:
            pass

    # 2) 정기 refresh(30분): 시즌을 DB로 추정해서 1회 갱신
    # - 이미 트리거로 처리한 (lid,season)은 중복 호출 피함
    for lid in league_ids:
        season = _resolve_season_for_league_from_db(lid)
        if season is None:
            continue
        if (lid, season) in processed_pairs:
            continue
        try:
            rows = fetch_standings(s, lid, season)
            n = upsert_standings_rows(lid, season, rows)
            total_rows += n
            print(f"[standings_worker] periodic_refresh league={lid} season={season} rows={len(rows)} upserted={n}")
        except Exception as e:
            print(f"[standings_worker] periodic_refresh league={lid} season={season} err: {e}", file=sys.stderr)

    # 3) TTL 정리
    try:
        cleanup_old_rows()
    except Exception:
        pass

    return total_rows


def run_once_bracket() -> int:
    """
    ✅ Round/Bracket 워커:
    - 60분 주기 + FT 트리거 즉시 1회
    - 역할:
      1) /fixtures?id= 로 league.round 및 raw 채우기(빈 것만, limit)
      2) DB matches 기반으로 tournament_ties 생성/갱신
    - 트리거 소비 방식: B(bracket_consumed_utc)
    """
    if not API_KEY:
        print("[bracket_worker] APIFOOTBALL_KEY(env) 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    league_ids = parse_live_leagues(LIVE_LEAGUES_ENV)
    if not league_ids:
        print("[bracket_worker] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return 0

    if not hasattr(run_once_bracket, "_ddl_done"):
        ensure_ft_triggers_table()
        ensure_tournament_ties_table()
        run_once_bracket._ddl_done = True  # type: ignore[attr-defined]

    s = _session()

    # 1) 트리거 우선 처리
    triggers = _select_unconsumed_triggers("bracket", limit=60)
    processed_pairs: Set[Tuple[int, int]] = set()
    for t in triggers:
        lid = safe_int(t.get("league_id"))
        season = safe_int(t.get("season"))
        if lid is None or season is None:
            continue
        processed_pairs.add((lid, season))

    total = 0
    for (lid, season) in sorted(processed_pairs):
        try:
            filled = bracket_fill_missing_rounds(s, lid, season, limit=60)
            up = build_and_upsert_bracket_for_league_season(lid, season)
            total += (filled + up)
            print(f"[bracket_worker] trigger_run league={lid} season={season} filled_round={filled} upserted_ties={up}")
        except Exception as e:
            print(f"[bracket_worker] trigger_run league={lid} season={season} err: {e}", file=sys.stderr)

    # 트리거 소비
    if triggers:
        fids = [int(x.get("fixture_id")) for x in triggers if safe_int(x.get("fixture_id")) is not None]
        try:
            _mark_triggers_consumed("bracket", fids)
        except Exception:
            pass

    # 2) 정기(60분): 시즌 추정 후 실행(트리거로 처리한 것 제외)
    for lid in league_ids:
        season = _resolve_season_for_league_from_db(lid)
        if season is None:
            continue
        if (lid, season) in processed_pairs:
            continue
        try:
            filled = bracket_fill_missing_rounds(s, lid, season, limit=40)
            up = build_and_upsert_bracket_for_league_season(lid, season)
            total += (filled + up)
            print(f"[bracket_worker] periodic_run league={lid} season={season} filled_round={filled} upserted_ties={up}")
        except Exception as e:
            print(f"[bracket_worker] periodic_run league={lid} season={season} err: {e}", file=sys.stderr)

    # 3) TTL 정리
    try:
        cleanup_old_rows()
    except Exception:
        pass

    return total



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
    반환: insert된 row 수
    """
    cols = set(_get_table_columns("match_events"))
    if not cols:
        return 0

    # ✅ 빈 배열이면 "기다림"(기존 DB 유지)
    if not events:
        return 0

    # ✅ 레이스 차단: "live 역할이 아닐 때만" INPLAY면 스킵
    # - live role에서는 INPLAY 타임라인을 정상적으로 써야 함
    # - backfill/watchdog/postmatch 경로에서만 INPLAY 건드리지 않도록 보호
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
        # 상태 조회 실패 시에는 안전하게 기존 동작(쓰기) 유지
        pass


    # 최소 필수(있을 때만 사용)
    def has(c: str) -> bool:
        return c.lower() in cols

    # 컬럼명 호환(둘 중 하나 존재)
    col_extra = "extra" if has("extra") else ("time_extra" if has("time_extra") else None)

    inserted = 0

    # ✅ events가 있을 때만 기존 fixture 이벤트 삭제(스냅샷 교체)
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

        # ✅ FT 트리거/브라켓 스키마 자동 추가(기존 기능 영향 없음)
        ensure_ft_triggers_table()
        ensure_tournament_ties_table()

        run_once._ddl_done = True  # type: ignore[attr-defined]


    now = now_utc()
    fetched_at = now
    s = _session()

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

            # ✅ FT 트리거 기록(B안): 어디서 FT가 감지되든 standings/bracket 워커가 소비
            if sg2 == "FINISHED":
                try:
                    enqueue_ft_trigger(fixture_id, lid, season, finished_iso_utc=iso_utc(now))
                except Exception:
                    pass


            if sg2 == "INPLAY":
                total_inplay += 1

                try:
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

                    print(f"      [live_all_events] fixture_id={fixture_id} events={len(events)} inserted={inserted}")
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
    _scan_fixtures_for_dates(live_dates, mode_tag="live", forced_interval=None)

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

        # ✅ FT 트리거/브라켓 스키마 자동 추가(기존 기능 영향 없음)
        ensure_ft_triggers_table()
        ensure_tournament_ties_table()

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

    print(f"[backfill_worker] done. total_fixtures={total_fixtures}")
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

        # ✅ FT 트리거/브라켓 스키마 자동 추가(기존 기능 영향 없음)
        ensure_ft_triggers_table()
        ensure_tournament_ties_table()

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
    - live: detect_interval(기존 10초)로 run_once() (핫패스)
    - backfill: 1시간 주기(기본 DATE_SCAN_INTERVAL_SEC)로 run_once_backfill()
    - watchdog: 60초 주기(기본 WATCHDOG_INTERVAL_SEC)로 run_once_watchdog()
    - standings: 30분 주기 + FT 트리거 즉시(폴링)로 run_once_standings()
    - bracket: 60분 주기 + FT 트리거 즉시(폴링)로 run_once_bracket()
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
        # ✅ 30분 주기 + FT 트리거 즉시성(짧은 폴링)
        print(f"[live_status_worker] start role=standings (periodic={STANDINGS_LOOP_SEC}s, poll={TRIGGER_POLL_SEC}s)")
        last_periodic = 0.0
        while True:
            try:
                now_ts = time.time()

                # 트리거 폴링: 자주 돌려도 run_once_standings 내부가 '미소비 트리거만' 처리하므로 안전
                run_once_standings()

                # periodic 간격 강제(너무 자주 전체 갱신하지 않게)
                if (now_ts - last_periodic) >= float(STANDINGS_LOOP_SEC):
                    last_periodic = now_ts
                    # periodic도 run_once_standings 내부에서 처리(트리거 처리 후 남은 리그 갱신)
                    run_once_standings()

            except Exception:
                traceback.print_exc()

            time.sleep(max(5, int(TRIGGER_POLL_SEC)))

    elif role == "bracket":
        # ✅ 60분 주기 + FT 트리거 즉시성(짧은 폴링)
        print(f"[live_status_worker] start role=bracket (periodic={BRACKET_LOOP_SEC}s, poll={TRIGGER_POLL_SEC}s)")
        last_periodic = 0.0
        while True:
            try:
                now_ts = time.time()

                run_once_bracket()

                if (now_ts - last_periodic) >= float(BRACKET_LOOP_SEC):
                    last_periodic = now_ts
                    run_once_bracket()

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


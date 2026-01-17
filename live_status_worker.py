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

from db import execute, fetch_one  # dev 스키마 확정 → 런타임 schema 조회 불필요 (dedupe용 read 추가)



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



# ─────────────────────────────────────
# DB Upsert
# ─────────────────────────────────────

# ─────────────────────────────────────
# (NEW) 이벤트 dedupe/state 테이블 (하키식)
# ─────────────────────────────────────

def ensure_event_dedupe_tables() -> None:
    """
    스키마 변경(기존 컬럼 수정/삭제) 없이, 추가 테이블만 생성한다.
    - match_event_states: fixture 단위 seen_keys / updated_at (운영/프룬)
    - match_event_key_map: (fixture_id, canonical_key) -> match_events.id(대표 row)
    """
    execute(
        """
        CREATE TABLE IF NOT EXISTS match_event_states (
          fixture_id BIGINT PRIMARY KEY,
          seen_keys TEXT[] NOT NULL DEFAULT '{}'::text[],
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    execute(
        """
        CREATE TABLE IF NOT EXISTS match_event_key_map (
          fixture_id BIGINT NOT NULL,
          canonical_key TEXT NOT NULL,
          event_id BIGINT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (fixture_id, canonical_key)
        );
        """
    )

    execute(
        "CREATE INDEX IF NOT EXISTS idx_match_event_key_map_event "
        "ON match_event_key_map (fixture_id, event_id);"
    )


def prune_event_dedupe_for_fixtures(fixture_ids: List[int]) -> None:
    """
    선택(3): FINISHED/OTHER 된 fixture에 대해 dedupe 상태를 정리한다.
    - 라이브 중복 수렴 목적이므로, 종료된 fixture는 map/state 유지할 필요가 거의 없음
    """
    if not fixture_ids:
        return

    # fixture_id = ANY(%s) 형태는 list 전달 가능(psycopg/pg)
    execute("DELETE FROM match_event_key_map WHERE fixture_id = ANY(%s)", (fixture_ids,))
    execute("DELETE FROM match_event_states WHERE fixture_id = ANY(%s)", (fixture_ids,))


def prune_event_dedupe_older_than(days: int = 3) -> None:
    """
    안전망: 혹시 FINISHED prune를 놓치더라도 일정 기간 지난 state/map은 정리.
    """
    try:
        d = int(days)
    except Exception:
        d = 3
    if d < 1:
        d = 1

    execute(
        """
        DELETE FROM match_event_states
        WHERE updated_at < now() - (%s::text || ' days')::interval
        """,
        (str(d),),
    )

    execute(
        """
        DELETE FROM match_event_key_map
        WHERE updated_at < now() - (%s::text || ' days')::interval
        """,
        (str(d),),
    )


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



def upsert_match_events_raw(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    """
    ✅ 확정(P1):
    - match_events_raw는 API 원본(/fixtures/events response)을 "무조건 그대로" 저장한다.
    - 벤치/스태프 필터/중복 제거/정정 처리 등 "해석"은 match_events 쪽에서만 수행한다.
    """
    raw = json.dumps(events or [], ensure_ascii=False, separators=(",", ":"))
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
    (하키식으로 변경) 이벤트 저장을 canonical_key 기준으로 "UPDATE 수렴"시킨다.

    핵심:
    - canonical_key 생성 (player_id가 나중에 채워져도 동일 사건으로 수렴하도록 설계)
    - match_event_key_map에서 (fixture_id, canonical_key) -> 대표 event_id 조회
    - 있으면: match_events 해당 id row를 UPDATE(빈 값만 채움)
    - 없으면: INSERT 후 key_map 등록
    - match_event_states.seen_keys는 운영/프룬/디버그 용도로 tick마다 합쳐서 저장
    """

    def _norm(s: Optional[str]) -> str:
        if not s:
            return ""
        x = str(s).lower().strip()
        x = " ".join(x.split())
        for ch in ("'", '"', "`", ".", ",", ":", ";", "!", "?", "(", ")", "[", "]", "{", "}", "|"):
            x = x.replace(ch, "")
        return x

    def _safe_name(x: Any) -> str:
        s = safe_text(x) or ""
        s = _norm(s)
        return s

    def _goal_kind(detail_norm: str) -> str:
        # OG/PK 판정은 현재 코드(스코어 반영)에서 정상 동작 중이므로,
        # 이벤트 저장에서도 같은 표기 체계를 존중(문구 기반)
        if "own goal" in detail_norm:
            return "OG"
        if "pen" in detail_norm and ("goal" in detail_norm or "penalty" in detail_norm):
            return "P"
        return "N"

    def _card_kind(detail_norm: str) -> str:
        if detail_norm == "second yellow card":
            return "SY"
        if detail_norm == "red card":
            return "R"
        if detail_norm == "yellow card":
            return "Y"
        return detail_norm or "C"

    def _synthetic_id_from_key(key: str) -> int:
        import hashlib
        digest = hashlib.sha1(key.encode("utf-8")).digest()
        h64 = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
        if h64 == 0:
            h64 = 1
        return -h64

    def _is_bench_staff_card(t_id: Optional[int], p_id: Optional[int], ev_type: Optional[str]) -> bool:
        if _norm(ev_type) != "card":
            return False
        if t_id is None or p_id is None:
            return False

        st = LINEUPS_STATE.get(fixture_id) or {}

        # ✅ 라인업 확정 전에는 벤치/스태프 판정을 하지 않음(오탐 방지)
        if not st.get("lineups_ready"):
            return False

        pb = st.get("players_by_team") or {}
        ids = pb.get(t_id)

        if not isinstance(ids, set) or not ids:
            return False

        return p_id not in ids

    # fixture별 seen_keys(운영/프룬/디버그)
    seen_keys_in_tick: List[str] = []

    # ✅ Second Yellow가 있는 경우 같은 키의 Red Card는 스킵(레드 2장 표시 방지) 유지
    second_yellow_keys: set = set()
    for ev in events or []:
        tm = ev.get("time") or {}
        minute = safe_int(tm.get("elapsed"))
        if minute is None:
            continue
        extra0 = int(safe_int(tm.get("extra")) or 0)

        ev_type_norm = _norm(safe_text(ev.get("type")))
        if ev_type_norm != "card":
            continue

        team = ev.get("team") or {}
        player = ev.get("player") or {}
        t_id = safe_int(team.get("id"))
        p_id = safe_int(player.get("id"))
        if t_id is None or p_id is None:
            continue

        detail_norm = _norm(safe_text(ev.get("detail")))
        if detail_norm == "second yellow card":
            second_yellow_keys.add((int(minute), int(extra0), int(t_id), int(p_id)))

    for ev in events or []:
        team = ev.get("team") or {}
        player = ev.get("player") or {}
        assist = ev.get("assist") or {}

        t_id = safe_int(team.get("id"))
        p_id = safe_int(player.get("id"))
        a_id = safe_int(assist.get("id"))

        ev_type = safe_text(ev.get("type"))
        detail = safe_text(ev.get("detail"))
        comments = safe_text(ev.get("comments"))

        # ---- 벤치/스태프 Card 차단 (기존 정책 유지) ----
        if _is_bench_staff_card(t_id, p_id, ev_type):
            continue

        tm = ev.get("time") or {}
        minute = safe_int(tm.get("elapsed"))
        extra0 = int(safe_int(tm.get("extra")) or 0)
        if minute is None:
            continue

        ev_type_norm = _norm(ev_type)
        detail_norm = _norm(detail)

        # ✅ Second Yellow가 있으면 같은 키의 Red Card는 스킵(레드 2장 표시 방지) 유지
        if ev_type_norm == "card" and t_id is not None and p_id is not None:
            k = (int(minute), int(extra0), int(t_id), int(p_id))
            if (detail_norm == "red card") and (k in second_yellow_keys):
                continue

        # ---- substitution 매핑(P5 유지): player=OUT / assist=IN ----
        player_in_id = None
        player_in_name = None
        if ev_type_norm in ("subst", "substitution", "sub"):
            player_in_id = a_id
            player_in_name = safe_text(assist.get("name"))

        # ---- canonical_key 생성(핵심) ----
        # player_id가 나중에 채워져도 같은 사건으로 수렴하도록:
        # - goal은 player_id를 키에서 제외(대신 player_name 기반으로 안정화)
        # - card/subst는 가능하면 player_name(없으면 id) 사용
        pname = _safe_name((player.get("name") if isinstance(player, dict) else None))
        aname = _safe_name((assist.get("name") if isinstance(assist, dict) else None))

        canonical_key = ""
        if ev_type_norm == "goal":
            kind = _goal_kind(detail_norm)
            # goal: player_id는 제외(늦게 채워져도 수렴), player_name은 안정 discriminator로 사용
            canonical_key = f"G|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{kind}|{pname}"
        elif ev_type_norm == "card":
            ck = _card_kind(detail_norm)
            # card: player_id가 있으면 함께 쓰되, 없을 때도 수렴하게 name도 포함
            canonical_key = f"C|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{ck}|{int(p_id or 0)}|{pname}"
        elif ev_type_norm in ("subst", "substitution", "sub"):
            canonical_key = f"S|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{int(p_id or 0)}|{pname}|{int(player_in_id or 0)}|{_safe_name(player_in_name)}"
        elif ev_type_norm == "var":
            canonical_key = f"V|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{detail_norm}"
        else:
            # fallback: 너무 많은 필드(assist/comments 등)는 중복 원인이 될 수 있어 최소화
            canonical_key = f"E|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{ev_type_norm}|{detail_norm}|{pname}|{aname}"

        seen_keys_in_tick.append(canonical_key)

        # ---- 대표 event_id 결정: key_map 우선 ----
        mapped = fetch_one(
            """
            SELECT event_id
            FROM match_event_key_map
            WHERE fixture_id=%s AND canonical_key=%s
            """,
            (fixture_id, canonical_key),
        )

        mapped_event_id: Optional[int] = None
        if isinstance(mapped, dict):
            mapped_event_id = safe_int(mapped.get("event_id"))

        incoming_id = safe_int(ev.get("id"))
        if mapped_event_id is not None:
            ev_id_used = mapped_event_id
        else:
            ev_id_used = incoming_id if incoming_id is not None else _synthetic_id_from_key(canonical_key)

        # ---- match_events INSERT/UPDATE 수렴 ----
        # 규칙:
        # - NULL/빈 값만 새 값으로 채움
        # - 기존 값이 있으면 유지(덮어쓰지 않음)
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
            ON CONFLICT (id) DO UPDATE SET
                fixture_id = COALESCE(match_events.fixture_id, EXCLUDED.fixture_id),
                team_id    = COALESCE(match_events.team_id, EXCLUDED.team_id),
                player_id  = COALESCE(match_events.player_id, EXCLUDED.player_id),

                type = CASE
                         WHEN match_events.type IS NULL OR match_events.type = '' THEN EXCLUDED.type
                         ELSE match_events.type
                       END,
                detail = CASE
                           WHEN match_events.detail IS NULL OR match_events.detail = '' THEN EXCLUDED.detail
                           ELSE match_events.detail
                         END,

                assist_player_id = COALESCE(match_events.assist_player_id, EXCLUDED.assist_player_id),
                assist_name      = COALESCE(match_events.assist_name, EXCLUDED.assist_name),
                player_in_id     = COALESCE(match_events.player_in_id, EXCLUDED.player_in_id),
                player_in_name   = COALESCE(match_events.player_in_name, EXCLUDED.player_in_name)
            """,
            (
                ev_id_used,
                fixture_id,
                t_id,
                p_id,
                ev_type,
                detail,
                minute,
                extra0,
                a_id,
                safe_text(assist.get("name")),
                player_in_id,
                player_in_name,
            ),
        )

        # ---- key_map upsert (event_id는 최초 값을 유지) ----
        # mapped가 없던 경우에만 등록 시도
        if mapped_event_id is None:
            execute(
                """
                INSERT INTO match_event_key_map (fixture_id, canonical_key, event_id, created_at, updated_at)
                VALUES (%s, %s, %s, now(), now())
                ON CONFLICT (fixture_id, canonical_key)
                DO UPDATE SET updated_at = now()
                """,
                (fixture_id, canonical_key, ev_id_used),
            )

    # ---- match_event_states 갱신(운영/프룬/디버그) ----
    if seen_keys_in_tick:
        # DISTINCT merge: 기존 배열 + 이번 배열을 합친 뒤 unique로 재구성
        execute(
            """
            INSERT INTO match_event_states (fixture_id, seen_keys, updated_at)
            VALUES (%s, %s::text[], now())
            ON CONFLICT (fixture_id) DO UPDATE SET
              seen_keys = (
                SELECT array_agg(DISTINCT x)
                FROM unnest(match_event_states.seen_keys || EXCLUDED.seen_keys) AS x
              ),
              updated_at = now()
            """,
            (fixture_id, seen_keys_in_tick),
        )
    else:
        # 이벤트가 비어도 updated_at은 찍어두면 프룬에 도움이 됨
        execute(
            """
            INSERT INTO match_event_states (fixture_id, seen_keys, updated_at)
            VALUES (%s, '{}'::text[], now())
            ON CONFLICT (fixture_id) DO UPDATE SET updated_at = now()
            """,
            (fixture_id,),
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
# 이벤트 기반 스코어 보정 (정교화 핵심)
# ─────────────────────────────────────

def calc_score_from_events(
    events: List[Dict[str, Any]],
    home_id: int,
    away_id: int,
    hint_home_ft: Optional[int] = None,
    hint_away_ft: Optional[int] = None,
) -> Tuple[int, int]:
    """
    Goal + Var 이벤트를 함께 사용해서 "최종 득점"을 계산한다.

    ✅ Var:
       - Goal Disallowed / Goal cancelled / No Goal  => 직전 Goal 1개를 취소 처리
       - Goal confirmed                              => 유지(아무것도 안 함)
    ✅ Missed Penalty(실축)는 득점에서 제외
    ✅ Own Goal(OG) 처리:
       - 공급자(team_id)가 '자책한 팀'으로 올 수도, '득점 인정된 팀'으로 올 수도 있어 케이스가 섞임
       - 그래서 OG를 무조건 flip 하지 않고,
         (1) OG flip 안한 점수, (2) OG flip 한 점수 두 가지를 모두 계산한 뒤
         /fixtures goals(hint_home_ft/hint_away_ft)와 더 가까운 쪽을 선택한다.
       - hint가 없으면 flip 하지 않는 쪽을 기본으로 사용(보수적).
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
        elv = el if el is not None else 10**9
        exv = ex if ex is not None else 0
        return (elv, exv, fallback_idx)

    invalid_markers = (
        "cancel",
        "disallow",
        "no goal",
        "offside",
        "foul",
        "annul",
        "null",
    )

    # goals: 득점 후보 리스트(Var로 취소되면 cancelled=True)
    goals: List[Dict[str, Any]] = []

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

        goals.append(
            {
                "team_id": team_id,          # 이벤트 team_id (공급자 기준)
                "is_og": bool(is_og),
                "elapsed": elapsed,
                "extra": extra,
                "cancelled": False,
            }
        )

    def _apply_var(ev: Dict[str, Any]) -> None:
        detail = _norm(ev.get("detail"))
        if not detail:
            return

        is_disallow = ("goal disallowed" in detail) or ("goal cancelled" in detail) or ("no goal" in detail)
        is_confirm = "goal confirmed" in detail
        if not (is_disallow or is_confirm):
            return
        if is_confirm:
            return

        team = ev.get("team") or {}
        var_team_id = safe_int(team.get("id"))
        tm = ev.get("time") or {}
        var_elapsed = safe_int(tm.get("elapsed"))

        # 보수적 취소: elapsed 없으면 취소하지 않음
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
                    continue
                if abs(g_el - var_elapsed) > max_delta:
                    continue

                # 팀 매칭 우선(가능하면)
                if var_team_id is not None:
                    if g.get("team_id") == var_team_id:
                        return i
                    if best is None:
                        best = i
                else:
                    return i
            return best

        best_idx = _pick_cancel_idx(0)
        if best_idx is None:
            best_idx = _pick_cancel_idx(1)
        if best_idx is None:
            best_idx = _pick_cancel_idx(2)

        if best_idx is not None:
            goals[best_idx]["cancelled"] = True

    for ev in evs:
        ev_type = _norm(ev.get("type"))
        if ev_type == "goal":
            _add_goal(ev)
        elif ev_type == "var":
            _apply_var(ev)

    def _sum_scores(flip_og: bool) -> Tuple[int, int]:
        h = 0
        a = 0
        for g in goals:
            if g.get("cancelled"):
                continue
            tid = g.get("team_id")
            is_og = bool(g.get("is_og"))

            scoring_tid = tid
            if flip_og and is_og:
                if tid == home_id:
                    scoring_tid = away_id
                elif tid == away_id:
                    scoring_tid = home_id

            if scoring_tid == home_id:
                h += 1
            elif scoring_tid == away_id:
                a += 1
        return h, a

    # 두 방식 계산
    h0, a0 = _sum_scores(flip_og=False)
    h1, a1 = _sum_scores(flip_og=True)

    # hint가 있으면 더 가까운 쪽 선택
    if hint_home_ft is not None and hint_away_ft is not None:
        d0 = abs(h0 - hint_home_ft) + abs(a0 - hint_away_ft)
        d1 = abs(h1 - hint_home_ft) + abs(a1 - hint_away_ft)
        if d1 < d0:
            return h1, a1
        return h0, a0

    # hint 없으면 보수적으로 flip 안함
    return h0, a0





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

    # ---- 공통: 과호출 방지 쿨다운(초) ----
    # interval=10초라서, lineups는 20초 정도만 쉬어도 충분히 안정적
    COOLDOWN_SEC = 20
    now_ts = time.time()
    last_try = float(st.get("last_try_ts") or 0.0)
    if (now_ts - last_try) < COOLDOWN_SEC:
        # 다만 UPCOMING 슬롯(-60/-10)은 1회성이라 쿨다운 걸리지 않게 아래에서 별도 처리
        pass

    # ─────────────────────────────────────
    # UPCOMING: -60 / -10 슬롯은 1회만
    # ─────────────────────────────────────
    if kickoff and status_group == "UPCOMING":
        mins = int((kickoff - nowu).total_seconds() / 60)

        # -60 슬롯: 59~61분 사이
        if (59 <= mins <= 61) and not st.get("slot60"):
            st["slot60"] = True
            try:
                st["last_try_ts"] = time.time()
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
                st["last_try_ts"] = time.time()
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

def run_once() -> None:
    # ✅ (NEW) dedupe 테이블은 1회 ensure
    if not hasattr(run_once, "_dedupe_tables_ok"):
        ensure_event_dedupe_tables()
        run_once._dedupe_tables_ok = True  # type: ignore[attr-defined]

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

                        # 이벤트 기반 스코어 계산(정교화) - OG/VAR/실축PK 처리 유지
                        goals_obj = (item.get("goals") or {})
                        hint_h = safe_int(goals_obj.get("home"))
                        hint_a = safe_int(goals_obj.get("away"))

                        h, a = calc_score_from_events(events, home_id, away_id, hint_h, hint_a)

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
    # (6) 런타임 캐시 prune (메모리 누적 방지) + (NEW) DB dedupe prune
    # ─────────────────────────────────────
    finished_ids: List[int] = []
    try:
        # FINISHED/OTHER는 더 이상 필요 없으므로 캐시 제거
        for fid, g in list(fixture_groups.items()):
            if g in ("FINISHED", "OTHER"):
                finished_ids.append(int(fid))

                LAST_STATS_SYNC.pop(fid, None)
                LINEUPS_STATE.pop(fid, None)

        # 아주 오래된 LINEUPS_STATE도 정리(혹시 오늘/어제 범위를 벗어났을 때)
        if len(LINEUPS_STATE) > 3000:
            for fid in list(LINEUPS_STATE.keys())[: len(LINEUPS_STATE) - 2000]:
                LINEUPS_STATE.pop(fid, None)
    except Exception:
        pass

    # ✅ (선택3) FINISHED/OTHER fixture의 dedupe 상태/맵 prune
    try:
        if finished_ids:
            prune_event_dedupe_for_fixtures(finished_ids)
    except Exception:
        pass

    # ✅ 안전망: 오래된 dedupe 데이터 정리(누수 방지)
    try:
        prune_event_dedupe_older_than(days=3)
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

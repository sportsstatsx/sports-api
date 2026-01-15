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

from db import execute  # dev 스키마 확정 → 런타임 schema 조회 불필요



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

    추가(스키마 변경 없음):
    - upsert_match_events()가 Own Goal team_id를 '득점 인정팀'으로 뒤집을 수 있도록
      런타임 캐시(LINEUPS_STATE)에 home_id/away_id를 저장한다.
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

    # ✅ 런타임 캐시 저장(스키마 변경 없음): Own Goal team_id flip에 사용
    try:
        st_cache = _ensure_lineups_state(fixture_id)
        st_cache["home_id"] = int(home_id)
        st_cache["away_id"] = int(away_id)
    except Exception:
        pass

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
    추가:
    - 벤치/스태프 Card 이벤트는 raw에도 저장하지 않음(수집 차단)
    - ✅ 단, "라인업이 확정(lineups_ready)"된 경우에만 벤치/스태프 필터 적용
      (초반/불완전 라인업에서 정상 카드까지 오탐 차단되는 문제 방지)
    """

    def _norm(s: Optional[str]) -> str:
        if not s:
            return ""
        x = str(s).lower().strip()
        x = " ".join(x.split())
        return x

    def _is_bench_staff_card(ev: Dict[str, Any]) -> bool:
        # Card가 아니면 대상 아님
        ev_type = _norm(safe_text(ev.get("type")))
        if ev_type != "card":
            return False

        team = ev.get("team") or {}
        player = ev.get("player") or {}

        t_id = safe_int(team.get("id"))
        p_id = safe_int(player.get("id"))

        # player_id가 없으면 애매 -> 오탐 방지 위해 저장 유지
        if p_id is None or t_id is None:
            return False

        st = LINEUPS_STATE.get(fixture_id) or {}

        # ✅ 라인업이 확정되기 전에는 "벤치/스태프" 판정 자체를 하지 않음
        if not st.get("lineups_ready"):
            return False

        pb = st.get("players_by_team") or {}
        ids = pb.get(t_id)

        # 라인업 셋이 없으면 판단 불가 -> 오탐 방지 위해 저장 유지
        if not isinstance(ids, set) or not ids:
            return False

        # 라인업에 없는 player_id인 Card => 벤치/스태프 카드로 간주하여 차단
        return p_id not in ids

    filtered: List[Dict[str, Any]] = []
    for ev in events or []:
        if _is_bench_staff_card(ev):
            continue
        filtered.append(ev)

    raw = json.dumps(filtered, ensure_ascii=False, separators=(",", ":"))
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

    변경 포인트(스키마 변경 없음):
    1) ✅ 벤치/스태프 Card 차단은 lineups_ready=True 일 때만 적용(초반 오탐 방지)
    2) ✅ 레드카드 2장 표기 방지:
       - 같은 퇴장에 대해 'Second Yellow card' + 'Red Card'가 같이 오는 케이스에서
         Second Yellow가 있으면 같은 분/extra/팀/선수의 Red Card는 스킵 + DB에서도 삭제
       - 공급자가 같은 카드를 id만 바꿔서 재발급하는 케이스는 "카드 시그니처"로 dedupe + DB 정리
    3) ✅ signature dedupe 안정화:
       - extra는 None/0 흔들려서 extra0=coalesce로 통일
       - 카드 dedupe에서 assist_id 변동으로 중복 생성되는 문제 방지 위해 sig에서 a_id 제외
    4) ✅ Own Goal team_id 정규화(중요):
       - API-Sports /fixtures/events 에서 Own Goal은 team_id가 '자책한 팀(source team)'으로 내려옴
       - 앱 타임라인은 match_events.team_id를 득점팀으로 해석하므로,
         DB에는 '득점 인정팀(benefit team)'으로 뒤집어서 저장한다.
       - home/away는 upsert_match_row_from_fixture()에서 LINEUPS_STATE에 저장한 값을 사용한다.
    5) 기존 유지:
       - events.id 없으면 synthetic id(음수)
       - synthetic Goal 유령 정리
    """

    def _norm(s: Optional[str]) -> str:
        if not s:
            return ""
        x = str(s).lower().strip()
        x = " ".join(x.split())
        for ch in ("'", '"', "`", ".", ",", ":", ";", "!", "?", "(", ")", "[", "]", "{", "}", "|"):
            x = x.replace(ch, "")
        return x

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

    def _synthetic_event_id(
        fixture_id_: int,
        minute_: int,
        extra_: Optional[int],
        t_id_: Optional[int],
        p_id_: Optional[int],
        a_id_: Optional[int],
        ev_type_: Optional[str],
        detail_: Optional[str],
        player_name_: Optional[str],
        assist_name_: Optional[str],
        comments_: Optional[str],
    ) -> int:
        import hashlib

        key = "|".join(
            [
                str(fixture_id_),
                str(minute_),
                str(extra_ or 0),
                str(t_id_ or 0),
                str(p_id_ or 0),
                str(a_id_ or 0),
                _norm(ev_type_),
                _norm(detail_),
                _norm(player_name_),
                _norm(assist_name_),
                _norm(comments_),
            ]
        )

        digest = hashlib.sha1(key.encode("utf-8")).digest()
        h64 = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
        if h64 == 0:
            h64 = 1
        return -h64

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

    # ✅ 이번 fetch에서 본 Goal 이벤트 id 모음(유령 골 정리용)
    current_goal_ids: List[int] = []

    # ✅ 이번 fetch에서 "player_id 확정된 Card" 시그니처 모음(카드 중복 정리용)
    #   (minute, extra0, team_id, detail_norm, player_id)
    current_cards_min: List[int] = []
    current_cards_extra: List[int] = []
    current_cards_team: List[int] = []
    current_cards_detail: List[str] = []
    current_cards_player: List[int] = []

    # ✅ Second Yellow가 있는 경우 같은 키의 Red Card는 제거(레드 2장 표시 방지)
    # key: (minute, extra0, team_id, player_id)
    second_yellow_keys: set = set()
    second_yellow_min: List[int] = []
    second_yellow_extra: List[int] = []
    second_yellow_team: List[int] = []
    second_yellow_player: List[int] = []

    # 1-pass: second yellow 키 먼저 수집(순서 무관하게 red card 스킵 가능)
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
            k = (int(minute), int(extra0), int(t_id), int(p_id))
            second_yellow_keys.add(k)

    # 메인 처리
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

        tm = ev.get("time") or {}
        minute = safe_int(tm.get("elapsed"))
        extra = safe_int(tm.get("extra"))
        extra0 = int(extra or 0)

        if minute is None:
            continue

        ev_type_norm = _norm(ev_type)
        detail_norm = _norm(detail)

        # ---- 벤치/스태프 Card 차단 ----
        if _is_bench_staff_card(t_id, p_id, ev_type):
            continue

        # ✅ Second Yellow가 있으면 같은 키의 Red Card는 스킵(레드 2장 표시 방지)
        if ev_type_norm == "card":
            if t_id is not None and p_id is not None:
                k = (int(minute), int(extra0), int(t_id), int(p_id))
                if (detail_norm == "red card") and (k in second_yellow_keys):
                    continue
                if detail_norm == "second yellow card":
                    # 나중 DB 삭제용 배열도 채움
                    second_yellow_min.append(int(minute))
                    second_yellow_extra.append(int(extra0))
                    second_yellow_team.append(int(t_id))
                    second_yellow_player.append(int(p_id))

        # id 또는 synthetic id
        ev_id = safe_int(ev.get("id"))
        if ev_id is None:
            ev_id = _synthetic_event_id(
                fixture_id_=fixture_id,
                minute_=minute,
                extra_=extra,
                t_id_=t_id,
                p_id_=p_id,
                a_id_=a_id,
                ev_type_=ev_type,
                detail_=detail,
                player_name_=safe_text(player.get("name")),
                assist_name_=safe_text(assist.get("name")),
                comments_=comments,
            )

        # ✅ Goal이면 현재 Goal id 목록에 추가(정리용)
        if ev_type_norm == "goal":
            current_goal_ids.append(ev_id)

        # ✅ Card인데 player_id가 확정된 경우만 "정리 기준"으로 저장
        if ev_type_norm == "card":
            if t_id is not None and detail is not None and p_id is not None:
                current_cards_min.append(int(minute))
                current_cards_extra.append(int(extra0))
                current_cards_team.append(int(t_id))
                current_cards_detail.append(detail_norm)
                current_cards_player.append(int(p_id))

        # ✅ Own Goal team_id 정규화(여기가 핵심)
        # - API: team_id = 자책한 팀(source team)
        # - DB(match_events): team_id = 득점 인정팀(benefit team)으로 저장
        effective_team_id = t_id
        if ev_type_norm == "goal" and ("own goal" in detail_norm) and (t_id is not None):
            st_cache = LINEUPS_STATE.get(fixture_id) or {}
            hid = safe_int(st_cache.get("home_id"))
            aid = safe_int(st_cache.get("away_id"))
            if hid is not None and aid is not None:
                if t_id == hid:
                    effective_team_id = aid
                elif t_id == aid:
                    effective_team_id = hid

        # ✅ signature dedupe (id가 바뀌어도 동일 이벤트면 스킵)
        # - extra는 extra0로 통일(None/0 흔들림 방지)
        # - a_id는 카드에서 흔들려 중복을 만들 수 있어 제외
        # - team_id는 DB에 저장될 team_id 기준으로 dedupe 해야 앱 표시가 안정적
        sig = (int(minute), int(extra0), ev_type_norm, detail_norm, effective_team_id, p_id)
        prev_ts = seen.get(sig)
        if prev_ts is not None and (now_ts - prev_ts) < 600:
            continue
        seen[sig] = now_ts

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
                effective_team_id,  # ✅ OG면 반대팀으로 저장
                p_id,
                ev_type,
                detail,
                minute,
                extra0,  # ✅ DB에는 int로
                a_id,
                safe_text(assist.get("name")),
                player_in_id,
                player_in_name,
            ),
        )

    # ✅ 유령 골 정리:
    # - 현재 API 응답에 포함된 Goal id 목록에 없는 "synthetic Goal(id<0)"은 삭제
    # - current_goal_ids가 비었으면(Goal이 하나도 없으면) 정리하지 않음(오탐 방지)
    if current_goal_ids:
        execute(
            """
            DELETE FROM match_events
            WHERE fixture_id = %s
              AND id < 0
              AND LOWER(type) = 'goal'
              AND NOT (id = ANY(%s))
            """,
            (fixture_id, current_goal_ids),
        )

    # ✅ Second Yellow가 확정된 경우 같은 키의 Red Card는 DB에서도 제거(레드 2장 방지)
    if second_yellow_min:
        execute(
            r"""
            DELETE FROM match_events me
            USING (
              SELECT *
              FROM unnest(
                %s::int[],
                %s::int[],
                %s::int[],
                %s::int[]
              ) AS t(minute, extra0, team_id, player_id)
            ) cur
            WHERE me.fixture_id = %s
              AND LOWER(me.type) = 'card'
              AND me.minute = cur.minute
              AND COALESCE(me.extra, 0) = cur.extra0
              AND me.team_id = cur.team_id
              AND (
                    me.player_id = cur.player_id
                 OR me.player_id IS NULL
              )
              AND translate(
                    lower(regexp_replace(coalesce(me.detail,''), '\s+', ' ', 'g')),
                    '"`.,:;!?()[]{}|',
                    ''
                  ) = 'red card'
            """,
            (
                second_yellow_min,
                second_yellow_extra,
                second_yellow_team,
                second_yellow_player,
                fixture_id,
            ),
        )

    # ✅ 카드 중복 정리(보수적 + 실효):
    # - "player_id 확정된 Card" 시그니처가 현재 응답에 존재하면,
    #   DB에 동일 시그니처로 중복된 card row(positive/negative id 모두 포함)를 1개만 남기고 정리한다.
    # - 남길 id 우선순위: (양수 id 최소값) 우선, 없으면 (음수 id 중 가장 큰 값=0에 가까운 값)
    if current_cards_min:
        execute(
            r"""
            DELETE FROM match_events me
            USING (
              SELECT *
              FROM unnest(
                %s::int[],
                %s::int[],
                %s::int[],
                %s::text[],
                %s::int[]
              ) AS t(minute, extra0, team_id, detail_norm, player_id)
            ) cur
            WHERE me.fixture_id = %s
              AND LOWER(me.type) = 'card'
              AND me.minute = cur.minute
              AND COALESCE(me.extra, 0) = cur.extra0
              AND me.team_id = cur.team_id
              AND translate(
                    lower(regexp_replace(coalesce(me.detail,''), '\s+', ' ', 'g')),
                    '"`.,:;!?()[]{}|',
                    ''
                  ) = cur.detail_norm
              AND (
                    me.player_id = cur.player_id
                 OR me.player_id IS NULL
              )
              AND me.id <> (
                    SELECT COALESCE(
                               MIN(m2.id) FILTER (WHERE m2.id > 0),
                               MAX(m2.id)
                           )
                    FROM match_events m2
                    WHERE m2.fixture_id = me.fixture_id
                      AND LOWER(m2.type) = 'card'
                      AND m2.minute = cur.minute
                      AND COALESCE(m2.extra, 0) = cur.extra0
                      AND m2.team_id = cur.team_id
                      AND translate(
                            lower(regexp_replace(coalesce(m2.detail,''), '\s+', ' ', 'g')),
                            '"`.,:;!?()[]{}|',
                            ''
                          ) = cur.detail_norm
                      AND (
                            m2.player_id = cur.player_id
                         OR m2.player_id IS NULL
                      )
              )
            """,
            (
                current_cards_min,
                current_cards_extra,
                current_cards_team,
                current_cards_detail,
                current_cards_player,
                fixture_id,
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
) -> Tuple[int, int]:
    """
    Goal + Var 이벤트를 함께 사용해서 "최종 득점"을 계산한다.

    ✅ Goal 이벤트는 일단 득점 후보로 쌓는다.
    ✅ Var 이벤트 중
       - Goal Disallowed / Goal cancelled / No Goal  => 직전 Goal 1개를 취소 처리
       - Goal confirmed                              => 유지(아무것도 안 함)
    ✅ Missed Penalty(실축)는 득점에서 제외 (기존 유지)

    🔥 중요(너의 최신 파이프라인 기준):
    - upsert_match_events()에서 Own Goal(team_id)을 이미 "득점 인정팀(benefit team)" 기준으로 정규화해 저장한다.
    - 따라서 여기서는 Own Goal을 반대팀으로 뒤집으면 "2중 flip"이 발생해서 스코어가 반대로 깨진다.
    - 결론: calc_score_from_events()에서는 OG를 뒤집지 않고, team_id 그대로 득점팀으로 계산한다.
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

        # ✅ 핵심: OG 포함 모든 Goal은 team_id 그대로 득점팀으로 처리
        # (OG flip은 upsert_match_events()에서 이미 정규화된 파이프라인)
        scoring_team_id = team_id

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

                if var_team_id is not None:
                    if (g.get("source_team_id") == var_team_id) or (g.get("scoring_team_id") == var_team_id):
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

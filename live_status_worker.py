# live_status_worker.py (single-file live worker)
#
# 목표:
# - 이 파일 1개만으로 라이브 업데이트가 돌아가게 단순화
# - DB 스키마 변경 없음 (기존 테이블/컬럼/PK는 그대로)
# - /fixtures 기반 상태/스코어 업데이트 + 원본 raw 저장(match_fixtures_raw)
# - INPLAY 경기: /events 저장 + events 기반 스코어 "정교 보정"(취소골/실축PK 제외, OG 반영)
# - INPLAY 경기: /statistics 60초 쿨다운
# - lineups: 프리매치(-60/-10 슬롯 1회씩) + 킥오프 직후(elapsed<=15) 재시도 정책
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

from __future__ import annotations

import os
import sys
import time
import json
import traceback
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import execute, fetch_one


# ─────────────────────────────────────
# Config / Constants
# ─────────────────────────────────────

BASE = "https://v3.football.api-sports.io"
UA = "SportsStatsX-LiveWorker/2.0"

REQ_TIMEOUT = 12
REQ_RETRIES = 2

STATS_INTERVAL_SEC = 60  # stats 쿨다운

# 런타임 캐시(프로세스 메모리)
LAST_STATS_SYNC: Dict[int, float] = {}         # fixture_id -> last ts
LINEUPS_STATE: Dict[int, Dict[str, Any]] = {}  # fixture_id -> state


@dataclass
class Config:
    api_key: str
    live_leagues_env: str
    interval_sec: int

    # rate limit
    rate_limit_per_min: float
    rate_limit_burst: float

    # fixtures 캐시
    season_ttl_sec: int
    nofix_ttl_sec: int

    # postmatch 스케줄
    postmatch_prune_after_sec: int

    # lineups
    lineups_cooldown_sec: int

    # dedupe
    # - "tables": match_event_key_map/state 사용(기존 네 코드 방식)
    # - "off": 테이블 생성/사용 안 함(권장X, 중복 수렴 품질 떨어짐)
    event_dedupe_mode: str


def load_config() -> Config:
    api_key = (os.environ.get("APIFOOTBALL_KEY") or os.environ.get("API_FOOTBALL_KEY") or "").strip()
    live_leagues_env = os.environ.get("LIVE_LEAGUES", "")
    interval_sec = int(os.environ.get("LIVE_WORKER_INTERVAL_SEC", "10"))

    def _f(name: str, default: str) -> float:
        try:
            return float(os.environ.get(name, default) or default)
        except Exception:
            return float(default)

    mode = (os.environ.get("EVENT_DEDUPE_MODE", "tables") or "tables").strip().lower()
    if mode not in ("tables", "off"):
        mode = "tables"

    return Config(
        api_key=api_key,
        live_leagues_env=live_leagues_env,
        interval_sec=interval_sec,
        rate_limit_per_min=_f("RATE_LIMIT_PER_MIN", "0"),
        rate_limit_burst=_f("RATE_LIMIT_BURST", "0"),
        season_ttl_sec=60 * 60,          # 60분
        nofix_ttl_sec=60 * 10,           # 10분
        postmatch_prune_after_sec=6 * 60 * 60,  # 6시간
        lineups_cooldown_sec=20,
        event_dedupe_mode=mode,
    )


# ─────────────────────────────────────
# Utilities
# ─────────────────────────────────────

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(dtobj: dt.datetime) -> str:
    x = dtobj.astimezone(dt.timezone.utc)
    return x.replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
    # dedupe (stable)
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
    UTC 00~03시는 어제 경기(자정 넘어가는 경기)가 INPLAY/FT로 남아있을 가능성이 높음
    """
    now = now_utc()
    today = now.date()
    dates = [today.isoformat()]
    if now.hour <= 3:
        dates.insert(0, (today - dt.timedelta(days=1)).isoformat())
    return dates


def infer_season_candidates(date_str: str) -> List[int]:
    y = int(date_str[:4])
    return [y, y - 1, y + 1]


def map_status_group(short_code: Optional[str]) -> str:
    code = (short_code or "").upper().strip()

    if code in ("NS", "TBD"):
        return "UPCOMING"

    if code in ("1H", "2H", "ET", "P", "BT", "INT", "LIVE", "HT"):
        return "INPLAY"

    if code in ("FT", "AET", "PEN"):
        return "FINISHED"

    if code in ("SUSP", "PST", "CANC", "ABD", "AWD", "WO"):
        return "OTHER"

    return "OTHER"


# ─────────────────────────────────────
# Rate Limiter (token bucket)
# ─────────────────────────────────────

class RateLimiter:
    def __init__(self, per_min: float, burst: float) -> None:
        self.rate = (per_min / 60.0) if per_min > 0 else 0.0
        self.max_tokens = burst if burst > 0 else (max(1.0, self.rate * 5) if self.rate > 0 else 0.0)
        self.tokens = float(self.max_tokens)
        self.ts = time.time()

    def acquire(self) -> None:
        if self.rate <= 0 or self.max_tokens <= 0:
            return  # unlimited

        now_ts = time.time()
        elapsed = max(0.0, now_ts - self.ts)
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate)
        self.ts = now_ts

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return

        need = 1.0 - self.tokens
        wait_sec = need / self.rate if self.rate > 0 else 0.25
        if wait_sec > 0:
            time.sleep(wait_sec)

        now_ts2 = time.time()
        elapsed2 = max(0.0, now_ts2 - self.ts)
        self.tokens = min(self.max_tokens, self.tokens + elapsed2 * self.rate)
        self.ts = now_ts2
        self.tokens = max(0.0, self.tokens - 1.0)


# ─────────────────────────────────────
# API Client
# ─────────────────────────────────────

class ApiClient:
    def __init__(self, api_key: str, rl: RateLimiter) -> None:
        self.api_key = api_key
        self.rl = rl
        self.sess = requests.Session()
        self.sess.headers.update(
            {
                "x-apisports-key": api_key,
                "Accept": "application/json",
                "User-Agent": UA,
            }
        )

    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{BASE}{path}"
        last_err: Optional[Exception] = None

        for _ in range(REQ_RETRIES + 1):
            try:
                self.rl.acquire()
                r = self.sess.get(url, params=params, timeout=REQ_TIMEOUT)

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
                return data if isinstance(data, dict) else {}
            except Exception as e:
                last_err = e
                time.sleep(0.4)

        raise last_err  # type: ignore

    def fixtures(self, league_id: int, date_str: str, season: int) -> List[Dict[str, Any]]:
        data = self.get("/fixtures", {"league": league_id, "date": date_str, "season": season})
        resp = data.get("response") if isinstance(data, dict) else None
        return resp if isinstance(resp, list) else []

    def events(self, fixture_id: int) -> List[Dict[str, Any]]:
        data = self.get("/fixtures/events", {"fixture": fixture_id})
        resp = data.get("response") if isinstance(data, dict) else None
        return resp if isinstance(resp, list) else []

    def statistics(self, fixture_id: int) -> List[Dict[str, Any]]:
        data = self.get("/fixtures/statistics", {"fixture": fixture_id})
        resp = data.get("response") if isinstance(data, dict) else None
        return resp if isinstance(resp, list) else []

    def lineups(self, fixture_id: int) -> List[Dict[str, Any]]:
        data = self.get("/fixtures/lineups", {"fixture": fixture_id})
        resp = data.get("response") if isinstance(data, dict) else None
        return resp if isinstance(resp, list) else []


# ─────────────────────────────────────
# DB: event dedupe tables (optional)
# ─────────────────────────────────────

def ensure_event_dedupe_tables() -> None:
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
    if not fixture_ids:
        return
    execute("DELETE FROM match_event_key_map WHERE fixture_id = ANY(%s)", (fixture_ids,))
    execute("DELETE FROM match_event_states WHERE fixture_id = ANY(%s)", (fixture_ids,))


def prune_event_dedupe_older_than(days: int = 3) -> None:
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


# ─────────────────────────────────────
# DB Upserts: fixtures/matches/raw/stats/lineups
# ─────────────────────────────────────

def upsert_fixture_row(
    fixture_id: int,
    league_id: Optional[int],
    season: Optional[int],
    date_utc: Optional[str],
    status_short: Optional[str],
    status_group: Optional[str],
) -> None:
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

    st = fx.get("status") or {}
    status_short = safe_text(st.get("short")) or safe_text(st.get("code")) or ""
    status_long = safe_text(st.get("long")) or ""
    status_elapsed = safe_int(st.get("elapsed"))
    status_extra = safe_int(st.get("extra"))

    status_group = map_status_group(status_short)
    status = (status_short or "").strip() or "UNK"

    home = (teams.get("home") or {}) if isinstance(teams, dict) else {}
    away = (teams.get("away") or {}) if isinstance(teams, dict) else {}
    home_id = safe_int(home.get("id")) or 0
    away_id = safe_int(away.get("id")) or 0
    if home_id == 0 or away_id == 0:
        raise ValueError("home_id/away_id missing (matches.home_id/away_id NOT NULL)")

    home_ft = safe_int(goals.get("home")) if isinstance(goals, dict) else None
    away_ft = safe_int(goals.get("away")) if isinstance(goals, dict) else None

    ht = (score.get("halftime") or {}) if isinstance(score, dict) else {}
    home_ht = safe_int(ht.get("home"))
    away_ht = safe_int(ht.get("away"))

    elapsed = status_elapsed

    referee = safe_text(fx.get("referee"))
    fixture_timezone = safe_text(fx.get("timezone"))
    fixture_timestamp = safe_int(fx.get("timestamp"))

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


def upsert_match_team_stats(fixture_id: int, stats_resp: List[Dict[str, Any]]) -> None:
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


def _ensure_lineups_state(fixture_id: int) -> Dict[str, Any]:
    st = LINEUPS_STATE.get(fixture_id)
    if not st:
        st = {"slot60": False, "slot10": False, "success": False, "lineups_ready": False, "last_try_ts": 0.0, "players_by_team": {}}
        LINEUPS_STATE[fixture_id] = st
    return st


def upsert_match_lineups(fixture_id: int, lineups_resp: List[Dict[str, Any]], updated_at: dt.datetime) -> bool:
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

        try:
            ids, start_cnt, _sub_cnt = _extract_player_ids_and_counts(item)
            pb[team_id] = set(ids)
            if (start_cnt >= 11) or (len(ids) >= 11):
                ready_any = True
        except Exception:
            pass

    if ready_any:
        st["lineups_ready"] = True

    if not ok_any_write:
        return False
    return bool(ready_any)


def maybe_sync_lineups(
    api: ApiClient,
    fixture_id: int,
    date_utc: str,
    status_group: str,
    elapsed: Optional[int],
    now: dt.datetime,
    cooldown_sec: int,
) -> None:
    st = _ensure_lineups_state(fixture_id)

    # success + ready면 잠금
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
    now_ts = time.time()
    last_try = float(st.get("last_try_ts") or 0.0)

    # UPCOMING 슬롯은 1회성이므로 cooldown 강제하지 않음(아래에서 slot 플래그로 제어)
    # INPLAY 재시도는 cooldown 적용

    if kickoff and status_group == "UPCOMING":
        mins = int((kickoff - nowu).total_seconds() / 60)

        if (59 <= mins <= 61) and not st.get("slot60"):
            st["slot60"] = True
            try:
                st["last_try_ts"] = time.time()
                resp = api.lineups(fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)
                if ready:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} slot60 ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} slot60 err: {e}", file=sys.stderr)
            return

        if (9 <= mins <= 11) and not st.get("slot10"):
            st["slot10"] = True
            try:
                st["last_try_ts"] = time.time()
                resp = api.lineups(fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)
                if ready:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} slot10 ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} slot10 err: {e}", file=sys.stderr)
            return

        return

    if status_group == "INPLAY":
        el = elapsed if elapsed is not None else 0
        if 0 <= el <= 15:
            if (time.time() - last_try) < cooldown_sec:
                return
            try:
                st["last_try_ts"] = time.time()
                resp = api.lineups(fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)
                if ready:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} inplay(el={el}) ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} inplay err: {e}", file=sys.stderr)


# ─────────────────────────────────────
# Events: dedupe + upsert (DB-call 최소화 버전)
# ─────────────────────────────────────

def _norm_key(s: Optional[str]) -> str:
    if not s:
        return ""
    x = str(s).lower().strip()
    x = " ".join(x.split())
    for ch in ("'", '"', "`", ".", ",", ":", ";", "!", "?", "(", ")", "[", "]", "{", "}", "|"):
        x = x.replace(ch, "")
    return x


def _goal_kind(detail_norm: str) -> str:
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


def _time_key(ev: Dict[str, Any], fallback_idx: int) -> Tuple[int, int, int]:
    tm = ev.get("time") or {}
    el = safe_int(tm.get("elapsed"))
    ex = safe_int(tm.get("extra"))
    elv = el if el is not None else 10**9
    exv = ex if ex is not None else 0
    return (elv, exv, fallback_idx)


def _is_bench_staff_card(fixture_id: int, t_id: Optional[int], p_id: Optional[int], ev_type: Optional[str]) -> bool:
    if _norm_key(ev_type) != "card":
        return False
    if t_id is None or p_id is None:
        return False

    st = LINEUPS_STATE.get(fixture_id) or {}

    # 라인업 준비 전에는 벤치/스탭 판단 안 함(오탐 방지)
    if not st.get("lineups_ready"):
        return False

    pb = st.get("players_by_team") or {}
    ids = pb.get(t_id)

    if not isinstance(ids, set) or not ids:
        return False

    return p_id not in ids


class EventDedupeCache:
    """
    fixture_id 단위로 key_map을 "1번만" 로드해서
    이벤트마다 DB 조회하던 병목을 제거.
    """
    def __init__(self, fixture_id: int, enabled: bool) -> None:
        self.fixture_id = fixture_id
        self.enabled = enabled
        self.map: Dict[str, int] = {}  # canonical_key -> event_id

    def load(self) -> None:
        if not self.enabled:
            self.map = {}
            return
        row = fetch_one(
            """
            SELECT COALESCE(json_agg(json_build_object('k', canonical_key, 'id', event_id)), '[]'::json) AS arr
            FROM match_event_key_map
            WHERE fixture_id=%s
            """,
            (self.fixture_id,),
        )
        arr: Any = []
        try:
            if isinstance(row, dict):
                arr = row.get("arr")
            if isinstance(arr, str):
                arr = json.loads(arr)
            if not isinstance(arr, list):
                arr = []
        except Exception:
            arr = []

        m: Dict[str, int] = {}
        for r in arr:
            if not isinstance(r, dict):
                continue
            k = safe_text(r.get("k"))
            eid = safe_int(r.get("id"))
            if k and eid is not None:
                m[k] = int(eid)
        self.map = m

    def get(self, canonical_key: str) -> Optional[int]:
        return self.map.get(canonical_key)

    def put(self, canonical_key: str, event_id: int) -> None:
        self.map[canonical_key] = int(event_id)

    def keys_with_prefix(self, prefix: str) -> Dict[str, int]:
        # events가 많아도 보통 수백 이하라 prefix scan은 충분히 빠름
        out: Dict[str, int] = {}
        for k, v in self.map.items():
            if k.startswith(prefix):
                out[k] = v
        return out


def upsert_match_events(
    fixture_id: int,
    events: List[Dict[str, Any]],
    dedupe_mode: str,
) -> None:
    """
    - dedupe_mode="tables": match_event_key_map 기반으로 canonical_key -> event_id 수렴
    - dedupe_mode="off": key_map을 쓰지 않고 incoming_id 또는 synthetic_id로만 넣음(권장X)
    """
    dedupe_enabled = (dedupe_mode == "tables")
    cache = EventDedupeCache(fixture_id, enabled=dedupe_enabled)
    cache.load()

    seen_keys_in_tick: List[str] = []

    # Second Yellow 있으면 같은 키의 Red Card 스킵(중복 레드 표시 방지)
    second_yellow_keys: set = set()
    for ev in events or []:
        tm = ev.get("time") or {}
        minute = safe_int(tm.get("elapsed"))
        if minute is None:
            continue
        extra0 = int(safe_int(tm.get("extra")) or 0)

        ev_type_norm = _norm_key(safe_text(ev.get("type")))
        if ev_type_norm != "card":
            continue

        team = ev.get("team") or {}
        player = ev.get("player") or {}
        t_id = safe_int(team.get("id"))
        p_id = safe_int(player.get("id"))
        if t_id is None or p_id is None:
            continue

        detail_norm = _norm_key(safe_text(ev.get("detail")))
        if detail_norm == "second yellow card":
            second_yellow_keys.add((int(minute), int(extra0), int(t_id), int(p_id)))

    indexed = list(enumerate(events or []))
    indexed.sort(key=lambda pair: _time_key(pair[1], pair[0]))
    evs = [ev for _, ev in indexed]

    for ev in evs:
        team = ev.get("team") or {}
        player = ev.get("player") or {}
        assist = ev.get("assist") or {}

        t_id = safe_int(team.get("id"))
        p_id = safe_int(player.get("id"))
        a_id = safe_int(assist.get("id"))

        ev_type = safe_text(ev.get("type"))
        detail = safe_text(ev.get("detail"))

        # bench/staff card 차단
        if _is_bench_staff_card(fixture_id, t_id, p_id, ev_type):
            continue

        tm = ev.get("time") or {}
        minute = safe_int(tm.get("elapsed"))
        extra0 = int(safe_int(tm.get("extra")) or 0)
        if minute is None:
            continue

        ev_type_norm = _norm_key(ev_type)
        detail_norm = _norm_key(detail)

        # second yellow 있으면 red 스킵
        if ev_type_norm == "card" and t_id is not None and p_id is not None:
            k = (int(minute), int(extra0), int(t_id), int(p_id))
            if (detail_norm == "red card") and (k in second_yellow_keys):
                continue

        # substitution 매핑: player=OUT / assist=IN
        player_in_id = None
        player_in_name = None
        if ev_type_norm in ("subst", "substitution", "sub"):
            player_in_id = a_id
            player_in_name = safe_text(assist.get("name"))

        pname = _norm_key((player.get("name") if isinstance(player, dict) else None))
        aname = _norm_key((assist.get("name") if isinstance(assist, dict) else None))

        canonical_key = ""
        mapped_event_id: Optional[int] = None

        if ev_type_norm == "goal":
            kind = _goal_kind(detail_norm)
            prefix = f"G|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{kind}|"

            ex_map = cache.keys_with_prefix(prefix)

            want_pid_key = (prefix + f"pid:{int(p_id)}") if p_id is not None else ""
            want_name_key = (prefix + f"name:{pname}") if pname else ""

            chosen_event_id: Optional[int] = None

            if want_pid_key and want_pid_key in ex_map:
                canonical_key = want_pid_key
                chosen_event_id = ex_map[want_pid_key]
            elif want_name_key and want_name_key in ex_map:
                canonical_key = want_name_key
                chosen_event_id = ex_map[want_name_key]
            elif len(ex_map) == 1:
                only_k = next(iter(ex_map.keys()))
                canonical_key = only_k
                chosen_event_id = ex_map[only_k]
            else:
                if want_pid_key:
                    canonical_key = want_pid_key
                elif want_name_key:
                    canonical_key = want_name_key
                else:
                    seq_n = len(ex_map) + 1
                    canonical_key = prefix + f"seq:{seq_n}"

            seen_keys_in_tick.append(canonical_key)
            mapped_event_id = chosen_event_id

        elif ev_type_norm == "card":
            ck = _card_kind(detail_norm)
            canonical_key = f"C|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{ck}|{int(p_id or 0)}|{pname}"
            seen_keys_in_tick.append(canonical_key)
            mapped_event_id = cache.get(canonical_key)

        elif ev_type_norm in ("subst", "substitution", "sub"):
            canonical_key = f"S|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{int(p_id or 0)}|{pname}|{int(player_in_id or 0)}|{_norm_key(player_in_name)}"
            seen_keys_in_tick.append(canonical_key)
            mapped_event_id = cache.get(canonical_key)

        elif ev_type_norm == "var":
            canonical_key = f"V|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{detail_norm}"
            seen_keys_in_tick.append(canonical_key)
            mapped_event_id = cache.get(canonical_key)

        else:
            canonical_key = f"E|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{ev_type_norm}|{detail_norm}|{pname}|{aname}"
            seen_keys_in_tick.append(canonical_key)
            mapped_event_id = cache.get(canonical_key)

        incoming_id = safe_int(ev.get("id"))

        if dedupe_enabled:
            if mapped_event_id is not None:
                ev_id_used = mapped_event_id
            else:
                ev_id_used = incoming_id if incoming_id is not None else _synthetic_id_from_key(canonical_key)
        else:
            # 테이블 dedupe OFF: incoming_id 우선, 없으면 synthetic
            ev_id_used = incoming_id if incoming_id is not None else _synthetic_id_from_key(canonical_key)

        # match_events upsert
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

        # key_map update (tables mode)
        if dedupe_enabled:
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
                cache.put(canonical_key, ev_id_used)
            else:
                execute(
                    """
                    UPDATE match_event_key_map
                    SET updated_at = now()
                    WHERE fixture_id=%s AND canonical_key=%s
                    """,
                    (fixture_id, canonical_key),
                )

            # GOAL: seq -> pid/name 업그레이드 매핑
            if ev_type_norm == "goal" and ("|seq:" in canonical_key):
                kind = _goal_kind(detail_norm)
                prefix = f"G|{fixture_id}|{minute}|{extra0}|{int(t_id or 0)}|{kind}|"
                if p_id is not None:
                    better = prefix + f"pid:{int(p_id)}"
                    execute(
                        """
                        INSERT INTO match_event_key_map (fixture_id, canonical_key, event_id, created_at, updated_at)
                        VALUES (%s, %s, %s, now(), now())
                        ON CONFLICT (fixture_id, canonical_key)
                        DO UPDATE SET updated_at = now()
                        """,
                        (fixture_id, better, ev_id_used),
                    )
                    cache.put(better, ev_id_used)
                if pname:
                    better = prefix + f"name:{pname}"
                    execute(
                        """
                        INSERT INTO match_event_key_map (fixture_id, canonical_key, event_id, created_at, updated_at)
                        VALUES (%s, %s, %s, now(), now())
                        ON CONFLICT (fixture_id, canonical_key)
                        DO UPDATE SET updated_at = now()
                        """,
                        (fixture_id, better, ev_id_used),
                    )
                    cache.put(better, ev_id_used)

    # states update (tables mode)
    if dedupe_enabled:
        if seen_keys_in_tick:
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
            execute(
                """
                INSERT INTO match_event_states (fixture_id, seen_keys, updated_at)
                VALUES (%s, '{}'::text[], now())
                ON CONFLICT (fixture_id) DO UPDATE SET updated_at = now()
                """,
                (fixture_id,),
            )


# ─────────────────────────────────────
# Score: events 기반 보정
# ─────────────────────────────────────

def calc_score_from_events(
    events: List[Dict[str, Any]],
    home_id: int,
    away_id: int,
    hint_home_ft: Optional[int] = None,
    hint_away_ft: Optional[int] = None,
) -> Tuple[int, int]:
    def _n(s: Optional[str]) -> str:
        if not s:
            return ""
        x = str(s).lower().strip()
        x = " ".join(x.split())
        return x

    invalid_markers = ("cancel", "disallow", "no goal", "offside", "foul", "annul", "null")

    goals: List[Dict[str, Any]] = []

    indexed = list(enumerate(events or []))
    indexed.sort(key=lambda pair: _time_key(pair[1], pair[0]))
    evs = [ev for _, ev in indexed]

    def _add_goal(ev: Dict[str, Any]) -> None:
        detail = _n(ev.get("detail"))

        # missed penalty 제외
        if "missed penalty" in detail:
            return
        if ("miss" in detail) and ("pen" in detail):
            return

        # Goal.detail에 무효 문구가 붙는 케이스 방어 (OG는 예외)
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
                "team_id": team_id,
                "is_og": bool(is_og),
                "elapsed": elapsed,
                "extra": extra,
                "cancelled": False,
            }
        )

    def _apply_var(ev: Dict[str, Any]) -> None:
        detail = _n(ev.get("detail"))
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
        ev_type = _n(ev.get("type"))
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

    h0, a0 = _sum_scores(flip_og=False)
    h1, a1 = _sum_scores(flip_og=True)

    if hint_home_ft is not None and hint_away_ft is not None:
        d0 = abs(h0 - hint_home_ft) + abs(a0 - hint_away_ft)
        d1 = abs(h1 - hint_home_ft) + abs(a1 - hint_away_ft)
        if d1 < d0:
            return h1, a1
        return h0, a0

    return h0, a0


def update_score_inplay_only(fixture_id: int, status_group: str, home_goals: int, away_goals: int) -> None:
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


def update_score_any_status(fixture_id: int, home_goals: int, away_goals: int) -> None:
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
# Fixture cache (league/date -> season/no)
# ─────────────────────────────────────

class FixturesCache:
    def __init__(self) -> None:
        # key: (league_id, date_str) -> {"season": int|None, "no": bool, "exp": float}
        self._m: Dict[Tuple[int, str], Dict[str, Any]] = {}

    def prune(self, now_ts: float) -> None:
        for k, v in list(self._m.items()):
            if float(v.get("exp") or 0) < now_ts:
                del self._m[k]

    def get(self, key: Tuple[int, str]) -> Optional[Dict[str, Any]]:
        return self._m.get(key)

    def set(self, key: Tuple[int, str], season: Optional[int], no: bool, exp: float) -> None:
        self._m[key] = {"season": season, "no": bool(no), "exp": float(exp)}

    def pop(self, key: Tuple[int, str]) -> None:
        self._m.pop(key, None)


# ─────────────────────────────────────
# Postmatch scheduler (runtime)
# ─────────────────────────────────────

class PostMatchScheduler:
    def __init__(self) -> None:
        # fixture_id -> {"ft_seen_ts": float, "did_60": bool, "did_30m": bool, "last_run_ts": float}
        self.state: Dict[int, Dict[str, Any]] = {}

    def prune(self, now_ts: float, prune_after_sec: int) -> None:
        cutoff = now_ts - float(prune_after_sec)
        for fid, st in list(self.state.items()):
            ft_seen = float((st or {}).get("ft_seen_ts") or 0.0)
            last_run = float((st or {}).get("last_run_ts") or 0.0)
            base = max(ft_seen, last_run)
            if base and base < cutoff:
                del self.state[fid]

    def schedule_if_needed(
        self,
        api: ApiClient,
        fixture_id: int,
        home_id: int,
        away_id: int,
        item: Dict[str, Any],
        now_ts: float,
        dedupe_mode: str,
    ) -> None:
        st = self.state.get(fixture_id)
        if not isinstance(st, dict):
            st = {"ft_seen_ts": now_ts, "did_60": False, "did_30m": False, "last_run_ts": 0.0}
            self.state[fixture_id] = st

        if not st.get("ft_seen_ts"):
            st["ft_seen_ts"] = now_ts

        ft_seen_ts = float(st.get("ft_seen_ts") or now_ts)
        age = now_ts - ft_seen_ts

        if (age >= 60.0) and (not bool(st.get("did_60"))):
            self._full_fetch(api, fixture_id, home_id, away_id, item, "postmatch+60s", dedupe_mode)
            st["did_60"] = True
            st["last_run_ts"] = time.time()

        if (age >= 1800.0) and (not bool(st.get("did_30m"))):
            self._full_fetch(api, fixture_id, home_id, away_id, item, "postmatch+30m", dedupe_mode)
            st["did_30m"] = True
            st["last_run_ts"] = time.time()

    def _full_fetch(
        self,
        api: ApiClient,
        fixture_id: int,
        home_id: int,
        away_id: int,
        item: Dict[str, Any],
        tag: str,
        dedupe_mode: str,
    ) -> None:
        try:
            events = api.events(fixture_id)
            upsert_match_events_raw(fixture_id, events)
            upsert_match_events(fixture_id, events, dedupe_mode)

            goals_obj = (item.get("goals") or {})
            hint_h = safe_int(goals_obj.get("home"))
            hint_a = safe_int(goals_obj.get("away"))
            h, a = calc_score_from_events(events, home_id, away_id, hint_h, hint_a)
            update_score_any_status(fixture_id, h, a)

            print(f"      [{tag}/events] fixture_id={fixture_id} goals={h}:{a} events={len(events)}")
        except Exception as e:
            print(f"      [{tag}/events] fixture_id={fixture_id} err: {e}", file=sys.stderr)

        try:
            stats = api.statistics(fixture_id)
            upsert_match_team_stats(fixture_id, stats)
            LAST_STATS_SYNC[fixture_id] = time.time()
            print(f"      [{tag}/stats] fixture_id={fixture_id} updated")
        except Exception as e:
            print(f"      [{tag}/stats] fixture_id={fixture_id} err: {e}", file=sys.stderr)

        try:
            st = _ensure_lineups_state(fixture_id)
            if not st.get("lineups_ready"):
                resp = api.lineups(fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, now_utc())
                if ready:
                    st["success"] = True
                print(f"      [{tag}/lineups] fixture_id={fixture_id} ready={ready}")
        except Exception as e:
            print(f"      [{tag}/lineups] fixture_id={fixture_id} err: {e}", file=sys.stderr)


# ─────────────────────────────────────
# Main run_once
# ─────────────────────────────────────

def run_once() -> None:
    cfg = load_config()

    if not cfg.api_key:
        print("[live_status_worker] APIFOOTBALL_KEY(env) 가 비어있습니다. 종료.", file=sys.stderr)
        return

    league_ids = parse_live_leagues(cfg.live_leagues_env)
    if not league_ids:
        print("[live_status_worker] LIVE_LEAGUES env 가 비어있습니다. 종료.", file=sys.stderr)
        return

    # dedupe tables ensure (tables 모드일 때만)
    if cfg.event_dedupe_mode == "tables":
        if not hasattr(run_once, "_dedupe_tables_ok"):
            ensure_event_dedupe_tables()
            run_once._dedupe_tables_ok = True  # type: ignore[attr-defined]

    # runtime singletons
    if not hasattr(run_once, "_fixtures_cache"):
        run_once._fixtures_cache = FixturesCache()  # type: ignore[attr-defined]
    fc: FixturesCache = run_once._fixtures_cache  # type: ignore[attr-defined]

    if not hasattr(run_once, "_postmatch"):
        run_once._postmatch = PostMatchScheduler()  # type: ignore[attr-defined]
    postmatch: PostMatchScheduler = run_once._postmatch  # type: ignore[attr-defined]

    rl = RateLimiter(cfg.rate_limit_per_min, cfg.rate_limit_burst)
    api = ApiClient(cfg.api_key, rl)

    dates = target_dates_for_live()
    now = now_utc()
    fetched_at = now
    now_ts = time.time()

    fc.prune(now_ts)
    postmatch.prune(now_ts, cfg.postmatch_prune_after_sec)

    total_fixtures = 0
    total_inplay = 0

    fixture_groups: Dict[int, str] = {}

    for date_str in dates:
        for lid in league_ids:
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
                        rows = api.fixtures(lid, date_str, cached_season)
                        if rows:
                            fixtures = rows
                            used_season = cached_season
                        else:
                            fc.pop(cache_key)
                    except Exception as e:
                        fc.pop(cache_key)
                        print(f"  [fixtures] league={lid} date={date_str} season={cached_season} err: {e}", file=sys.stderr)

            if used_season is None:
                for season in infer_season_candidates(date_str):
                    try:
                        rows = api.fixtures(lid, date_str, season)
                        if rows:
                            fixtures = rows
                            used_season = season
                            fc.set(cache_key, season=season, no=False, exp=now_ts + cfg.season_ttl_sec)
                            break
                    except Exception as e:
                        print(f"  [fixtures] league={lid} date={date_str} season={season} err: {e}", file=sys.stderr)

            if used_season is None:
                fc.set(cache_key, season=None, no=True, exp=now_ts + cfg.nofix_ttl_sec)
                continue

            total_fixtures += len(fixtures)
            print(f"[fixtures] league={lid} date={date_str} season={used_season} count={len(fixtures)}")

            for item in fixtures:
                try:
                    fx = item.get("fixture") or {}
                    fid = safe_int(fx.get("id"))
                    if fid is None:
                        continue

                    st0 = fx.get("status") or {}
                    status_short = safe_text(st0.get("short")) or safe_text(st0.get("code")) or ""
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
                        maybe_sync_lineups(
                            api, fixture_id, date_utc, sg, elapsed, now, cooldown_sec=cfg.lineups_cooldown_sec
                        )
                    except Exception as lu_err:
                        print(f"      [lineups] fixture_id={fixture_id} policy err: {lu_err}", file=sys.stderr)

                    # FINISHED: postmatch schedule만
                    if sg == "FINISHED":
                        postmatch.schedule_if_needed(
                            api, fixture_id, home_id, away_id, item, now_ts=now_ts, dedupe_mode=cfg.event_dedupe_mode
                        )
                        continue

                    if sg != "INPLAY":
                        continue

                    total_inplay += 1

                    # events + score
                    try:
                        events = api.events(fixture_id)
                        upsert_match_events_raw(fixture_id, events)
                        upsert_match_events(fixture_id, events, cfg.event_dedupe_mode)

                        goals_obj = (item.get("goals") or {})
                        hint_h = safe_int(goals_obj.get("home"))
                        hint_a = safe_int(goals_obj.get("away"))

                        h, a = calc_score_from_events(events, home_id, away_id, hint_h, hint_a)
                        update_score_inplay_only(fixture_id, sg, h, a)

                        print(f"      [events] fixture_id={fixture_id} goals(events)={h}:{a} events={len(events)}")
                    except Exception as ev_err:
                        print(f"      [events] fixture_id={fixture_id} err: {ev_err}", file=sys.stderr)

                    # stats (cooldown)
                    try:
                        now_ts2 = time.time()
                        last_ts = LAST_STATS_SYNC.get(fixture_id)
                        if (last_ts is None) or ((now_ts2 - last_ts) >= STATS_INTERVAL_SEC):
                            stats = api.statistics(fixture_id)
                            upsert_match_team_stats(fixture_id, stats)
                            LAST_STATS_SYNC[fixture_id] = now_ts2
                            print(f"      [stats] fixture_id={fixture_id} updated")
                    except Exception as st_err:
                        print(f"      [stats] fixture_id={fixture_id} err: {st_err}", file=sys.stderr)

                except Exception as e:
                    print(f"  ! fixture 처리 중 에러: {e}", file=sys.stderr)

    # ─────────────────────────────────────
    # runtime cache prune + dedupe prune
    # ─────────────────────────────────────
    finished_ids: List[int] = []
    try:
        for fid, g in list(fixture_groups.items()):
            if g in ("FINISHED", "OTHER"):
                finished_ids.append(int(fid))
                LAST_STATS_SYNC.pop(fid, None)
                LINEUPS_STATE.pop(fid, None)

        if len(LINEUPS_STATE) > 3000:
            for fid in list(LINEUPS_STATE.keys())[: len(LINEUPS_STATE) - 2000]:
                LINEUPS_STATE.pop(fid, None)
    except Exception:
        pass

    # tables mode일 때만 prune
    if cfg.event_dedupe_mode == "tables":
        try:
            if finished_ids:
                prune_event_dedupe_for_fixtures(finished_ids)
        except Exception:
            pass

        try:
            prune_event_dedupe_older_than(days=3)
        except Exception:
            pass

    print(f"[live_status_worker] done. total_fixtures={total_fixtures}, inplay={total_inplay}")


# ─────────────────────────────────────
# Loop
# ─────────────────────────────────────

def loop() -> None:
    cfg = load_config()
    print(f"[live_status_worker] start (interval={cfg.interval_sec}s)")
    while True:
        try:
            run_once()
        except Exception:
            traceback.print_exc()
        time.sleep(cfg.interval_sec)


if __name__ == "__main__":
    loop()

# live_status_worker.py (single-file live worker)
#
# 목표:
# - 이 파일 1개만으로 라이브 업데이트가 돌아가게 단순화
# - DB 기존 스키마 변경 없음 (기존 테이블/컬럼/PK/타입 유지)
#   * 단, dedupe/state용 "추가 테이블 생성"은 허용
# - /fixtures 기반 상태/스코어 업데이트 + 원본 raw 저장(match_fixtures_raw)
# - INPLAY 경기: /events 저장 + events 기반 스코어 "정교 보정"(취소골/실축PK 제외, OG 반영)
# - INPLAY 경기: /statistics 60초 쿨다운
# - lineups: 프리매치(-60/-10 슬롯 1회씩) + 킥오프 직후(elapsed<=15) 재시도 정책
# - FINISHED: FT 최초 관측 시각 기준 +60초 / +30분에 1회씩 확정 수집
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
#
# (추가 테이블 - 기존 스키마 변경 없이 테이블 추가만)
# - match_event_states (fixture_id PK)
# - match_event_key_map ((fixture_id, canonical_key) PK)

import os
import sys
import time
import json
import traceback
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests

from db import execute, fetch_one


# ─────────────────────────────────────
# ENV / 상수
# ─────────────────────────────────────

API_KEY = os.environ.get("APIFOOTBALL_KEY") or os.environ.get("API_FOOTBALL_KEY")
LIVE_LEAGUES_ENV = os.environ.get("LIVE_LEAGUES", "")
INTERVAL_SEC = int(os.environ.get("LIVE_WORKER_INTERVAL_SEC", "10"))

BASE = "https://v3.football.api-sports.io"
UA = "SportsStatsX-LiveWorker/1.0"

STATS_INTERVAL_SEC = 60
REQ_TIMEOUT = 12
REQ_RETRIES = 2

# 이벤트 시간 보정 허용 범위(분)
EVENT_MINUTE_CORR_ALLOW = 10

# events 폴링 쿨다운(초) - INPLAY에서 /fixtures/events 과다 호출 방지
EVENTS_INTERVAL_SEC = int(os.environ.get("LIVE_EVENTS_INTERVAL_SEC", "20"))

# fixtures 폴링 쿨다운(초) - /fixtures 리그×날짜 매틱 호출 방지
FIXTURES_POLL_INTERVAL_SEC = int(os.environ.get("LIVE_FIXTURES_POLL_INTERVAL_SEC", "30"))

# dedupe prune 실행 주기(초) - 매틱 DELETE 방지
PRUNE_INTERVAL_SEC = int(os.environ.get("LIVE_DEDUPE_PRUNE_INTERVAL_SEC", "3600"))

# INPLAY 라인업 재시도 최대 횟수(0~15분 구간)
LINEUPS_INPLAY_MAX_TRIES = int(os.environ.get("LIVE_LINEUPS_INPLAY_MAX_TRIES", "8"))

# 최소값 안전장치
if EVENTS_INTERVAL_SEC < 5:
    EVENTS_INTERVAL_SEC = 5
if FIXTURES_POLL_INTERVAL_SEC < 10:
    FIXTURES_POLL_INTERVAL_SEC = 10
if PRUNE_INTERVAL_SEC < 300:
    PRUNE_INTERVAL_SEC = 300
if LINEUPS_INPLAY_MAX_TRIES < 1:
    LINEUPS_INPLAY_MAX_TRIES = 1



# ─────────────────────────────────────
# 런타임 캐시 (메모리)
# ─────────────────────────────────────

LAST_STATS_SYNC: Dict[int, float] = {}          # fixture_id -> last ts
LINEUPS_STATE: Dict[int, Dict[str, Any]] = {}   # fixture_id -> state dict
LAST_EVENTS_SYNC: Dict[int, float] = {}         # fixture_id -> last events poll ts



# ─────────────────────────────────────
# 유틸
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
    기본 UTC 오늘.
    UTC 00~03시는 자정 넘어가는 경기 누락 방지 위해 UTC 어제도 같이 조회.
    """
    now = now_utc()
    today = now.date()
    dates = [today.isoformat()]
    if now.hour <= 3:
        dates.insert(0, (today - dt.timedelta(days=1)).isoformat())
    return dates


def map_status_group(short_code: Optional[str]) -> str:
    code = (short_code or "").upper().strip()

    if code in ("NS", "TBD"):
        return "UPCOMING"

    # INPLAY (HT 포함)
    if code in ("1H", "2H", "ET", "P", "BT", "INT", "LIVE", "HT"):
        return "INPLAY"

    if code in ("FT", "AET", "PEN"):
        return "FINISHED"

    if code in ("SUSP", "PST", "CANC", "ABD", "AWD", "WO"):
        return "OTHER"

    return "OTHER"


def infer_season_candidates(date_str: str) -> List[int]:
    y = int(date_str[:4])
    return [y, y - 1, y + 1]


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


class _TokenBucket:
    def __init__(self) -> None:
        try:
            per_min = float(os.environ.get("RATE_LIMIT_PER_MIN", "0") or "0")
        except Exception:
            per_min = 0.0
        try:
            burst = float(os.environ.get("RATE_LIMIT_BURST", "0") or "0")
        except Exception:
            burst = 0.0

        self.rate = (per_min / 60.0) if per_min > 0 else 0.0
        if self.rate > 0:
            self.max_tokens = burst if burst > 0 else max(1.0, self.rate * 5)
        else:
            self.max_tokens = 0.0

        self.tokens = self.max_tokens
        self.ts = time.time()

    def acquire(self) -> None:
        if self.rate <= 0 or self.max_tokens <= 0:
            return

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


_RL = _TokenBucket()


def api_get(session: requests.Session, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE}{path}"
    last_err: Optional[Exception] = None

    for _ in range(REQ_RETRIES + 1):
        try:
            _RL.acquire()
            r = session.get(url, params=params, timeout=REQ_TIMEOUT)

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

# ─────────────────────────────────────
# Standings (Football) - FT 최초 관측 시 1회 갱신
# ─────────────────────────────────────

def fetch_standings(session: requests.Session, league_id: int, season: int) -> Dict[str, Any]:
    return api_get(session, "/standings", {"league": int(league_id), "season": int(season)})


def _db_table_columns(table_name: str) -> set:
    """
    standings 테이블 컬럼을 동적으로 확인해서,
    환경별(컬럼 추가/누락) 차이로 워커가 죽지 않게 한다.
    """
    try:
        # ✅ 상단 import 변경 없이, 함수 내부에서만 import
        from db import fetch_all  # type: ignore
    except Exception:
        return set()

    rows = fetch_all(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        """,
        (table_name,),
    )
    cols = set()
    for r in rows or []:
        if isinstance(r, dict) and r.get("column_name"):
            cols.add(r["column_name"])
    return cols


def _normalize_standings_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    /standings 응답을 standings 테이블 upsert용 row list로 평탄화.
    현재 DB 스키마 기준:
      PK: (league_id, season, group_name, team_id)
      cols: rank, points, goals_diff, played, win, draw, lose, goals_for, goals_against, form, updated_utc, description
    """
    out: List[Dict[str, Any]] = []
    resp = (payload or {}).get("response") or []
    if not isinstance(resp, list) or not resp:
        return out

    league = (resp[0] or {}).get("league") or {}
    blocks = league.get("standings") or []
    if not isinstance(blocks, list):
        return out

    for block in blocks:
        if not isinstance(block, list):
            continue
        for row in block:
            if not isinstance(row, dict):
                continue

            team = row.get("team") or {}
            all_ = row.get("all") or {}
            goals = (all_.get("goals") or {}) if isinstance(all_, dict) else {}

            team_id = safe_int(team.get("id"))
            if team_id is None:
                continue

            rank = safe_int(row.get("rank")) or 0
            played = safe_int(all_.get("played")) if isinstance(all_, dict) else None
            win = safe_int(all_.get("win")) if isinstance(all_, dict) else None
            draw = safe_int(all_.get("draw")) if isinstance(all_, dict) else None
            lose = safe_int(all_.get("lose")) if isinstance(all_, dict) else None

            goals_for = safe_int(goals.get("for")) if isinstance(goals, dict) else None
            goals_against = safe_int(goals.get("against")) if isinstance(goals, dict) else None

            goals_diff = safe_int(row.get("goalsDiff"))
            if goals_diff is None and goals_for is not None and goals_against is not None:
                goals_diff = goals_for - goals_against

            points = safe_int(row.get("points"))

            # API row에 group이 없으면 DB 기본값(Overall)로 맞추되,
            # PK에 포함이므로 None이면 "Overall"로 강제
            group_name = (safe_text(row.get("group")) or "Overall").strip() or "Overall"

            form = safe_text(row.get("form"))
            description = safe_text(row.get("description"))

            out.append(
                {
                    "group_name": group_name,
                    "rank": rank,
                    "team_id": int(team_id),
                    "points": points,
                    "goals_diff": goals_diff,
                    "played": played,
                    "win": win,
                    "draw": draw,
                    "lose": lose,
                    "goals_for": goals_for,
                    "goals_against": goals_against,
                    "form": form,
                    "description": description,
                }
            )

    return out



def refresh_standings_for_league(
    session: requests.Session,
    league_id: int,
    season: int,
) -> None:
    """
    리그+시즌 standings를 받아 standings 테이블에 upsert.
    ✅ 쿨다운(기본 5분)으로 FT 연속 처리/재시작에도 스팸 방지.
    """
    try:
        cooldown = int(os.environ.get("STANDINGS_REFRESH_MIN_INTERVAL_SEC", "300"))
    except Exception:
        cooldown = 300
    if cooldown < 30:
        cooldown = 30

    # 함수 attribute로 마지막 실행 ts 보관(전역 추가 없이)
    last_map = getattr(refresh_standings_for_league, "_last_map", None)
    if not isinstance(last_map, dict):
        last_map = {}
        setattr(refresh_standings_for_league, "_last_map", last_map)

    key = (int(league_id), int(season))
    now_ts = time.time()
    last_ts = float(last_map.get(key) or 0.0)
    if last_ts and (now_ts - last_ts) < cooldown:
        return

    cols = _db_table_columns("standings")
    if not cols:
        # 컬럼 조회 실패해도 최소 컬럼으로 시도할 수는 있지만,
        # 여기선 안전하게 스킵(테이블 없거나 db 모듈 미스 등)
        print(f"      [standings] skip: cannot read columns (table=standings)")
        last_map[key] = now_ts
        return

    try:
        payload = fetch_standings(session, league_id, season)
    except Exception as e:
        print(f"      [standings] fetch err league={league_id} season={season}: {e}", file=sys.stderr)
        last_map[key] = now_ts
        return

    rows = _normalize_standings_rows(payload)
    if not rows:
        print(f"      [standings] empty league={league_id} season={season}")
        last_map[key] = now_ts
        return

    # ✅ 현재 DB standings 스키마 기준으로 컬럼 구성
    # PK: (league_id, season, group_name, team_id)
    base_cols = ["league_id", "season", "group_name", "team_id", "rank"]

    opt_cols: List[str] = []
    # 아래 컬럼들은 테이블에 있으면 넣고, 없으면 안전하게 제외
    for c in (
        "points",
        "goals_diff",
        "played",
        "win",
        "draw",
        "lose",
        "goals_for",
        "goals_against",
        "form",
        "updated_utc",
        "description",
    ):
        if c in cols:
            opt_cols.append(c)

    use_cols = base_cols + opt_cols

    now_updated_utc = iso_utc(now_utc())

    placeholders: List[str] = []
    values: List[Any] = []
    for r in rows:
        placeholders.append("(" + ",".join(["%s"] * len(use_cols)) + ")")
        for c in use_cols:
            if c == "league_id":
                values.append(int(league_id))
            elif c == "season":
                values.append(int(season))
            elif c == "updated_utc":
                values.append(now_updated_utc)
            else:
                values.append(r.get(c))

    set_parts: List[str] = []
    for c in use_cols:
        if c in ("league_id", "season", "group_name", "team_id"):
            continue
        set_parts.append(f"{c}=EXCLUDED.{c}")

    sql = f"""
    INSERT INTO standings ({", ".join(use_cols)})
    VALUES {", ".join(placeholders)}
    ON CONFLICT (league_id, season, group_name, team_id)
    DO UPDATE SET {", ".join(set_parts)}
    """

    try:
        execute(sql, tuple(values))
        print(f"      [standings] refreshed league={league_id} season={season} rows={len(rows)}")
    except Exception as e:
        print(f"      [standings] upsert err league={league_id} season={season}: {e}", file=sys.stderr)


    last_map[key] = now_ts


def refresh_standings_for_fixture(
    session: requests.Session,
    fixture_id: int,
    league_id_hint: Optional[int] = None,
    season_hint: Optional[int] = None,
) -> None:
    """
    fixture_id 기준으로 league_id/season을 안정적으로 확보해서 standings refresh.
    - hint가 있으면 우선 사용
    - 없으면 matches에서 조회 (가장 확실)
    """
    lid = league_id_hint
    sea = season_hint

    if lid is None or sea is None:
        row = fetch_one(
            "SELECT league_id, season FROM matches WHERE fixture_id=%s",
            (fixture_id,),
        )
        if isinstance(row, dict):
            if lid is None:
                lid = safe_int(row.get("league_id"))
            if sea is None:
                sea = safe_int(row.get("season"))

    if lid is None or sea is None:
        return

    refresh_standings_for_league(session, int(lid), int(sea))



# ─────────────────────────────────────
# DB Dedupe Tables (추가 테이블 생성)
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

    # ✅ 재시작/상태유실에도 FINISHED postmatch가 재실행되지 않도록 DB에 상태 저장
    execute(
        """
        CREATE TABLE IF NOT EXISTS match_postmatch_states (
          fixture_id BIGINT PRIMARY KEY,
          ft_seen_at TIMESTAMPTZ NOT NULL,
          did_60 BOOLEAN NOT NULL DEFAULT FALSE,
          did_30m BOOLEAN NOT NULL DEFAULT FALSE,
          last_run_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    execute(
        "CREATE INDEX IF NOT EXISTS idx_match_postmatch_states_updated "
        "ON match_postmatch_states (updated_at);"
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
# DB Upsert: fixtures / matches / raw
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


# ─────────────────────────────────────
# 이벤트 dedupe/수렴 (중복/타임라인 붕괴 근본 해결)
# ─────────────────────────────────────

def upsert_match_events(fixture_id: int, events: List[Dict[str, Any]]) -> None:
    """
    ✅ 중복/타임라인 붕괴 근본 해결 (확정판)

    핵심 원인:
      - incoming_id(양수)가 나중에 생기면 기존 synthetic(음수)와 PK가 달라져 "새 row"가 추가됨
      - StableKey가 tick 내 순서(n)에 의존하면 순서가 바뀌는 순간 키가 흔들려 새 synthetic가 또 생성됨

    해결:
      1) event_id(PK)는 절대 incoming_id로 갈아타지 않는다.
         -> 항상 stable_key 기반(우리 쪽 안정 ID)로 수렴한다.
      2) n(occurrence)은 "시간 정렬" 기준으로 결정하고,
         DB에 이미 있던 stable_base|n:*가 있으면 우선 재사용한다.
      3) 과거에 남아있는 ID|... 키는 stable 키로 자동 마이그레이션한다(키 1개 정책 유지).
      4) 이미 DB에 쌓여있는 "incoming_id row"가 stable id와 중복이면 실시간으로 제거한다.
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
        return _norm(safe_text(x) or "")

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

    def _minute_bucket10(minute: Optional[int]) -> int:
        if minute is None:
            return 999
        if minute < 0:
            return 0
        return int(minute // 10)

    def _is_bench_staff_card(t_id: Optional[int], p_id: Optional[int], ev_type: Optional[str]) -> bool:
        if _norm(ev_type) != "card":
            return False
        if t_id is None or p_id is None:
            return False
        st = LINEUPS_STATE.get(fixture_id) or {}
        if not st.get("lineups_ready"):
            return False
        pb = st.get("players_by_team") or {}
        ids = pb.get(t_id)
        if not isinstance(ids, set) or not ids:
            return False
        return p_id not in ids

    # ───────────── key_map 1회 로드 ─────────────
    km_row = fetch_one(
        """
        SELECT COALESCE(json_agg(json_build_object('k', canonical_key, 'id', event_id)), '[]'::json) AS arr
        FROM match_event_key_map
        WHERE fixture_id=%s
        """,
        (fixture_id,),
    )

    km_list: List[Dict[str, Any]] = []
    try:
        if isinstance(km_row, dict) and km_row.get("arr") is not None:
            arr = km_row.get("arr")
            if isinstance(arr, str):
                km_list = json.loads(arr)
            elif isinstance(arr, list):
                km_list = arr
    except Exception:
        km_list = []

    key_to_id: Dict[str, int] = {}
    for r in km_list:
        if not isinstance(r, dict):
            continue
        k = safe_text(r.get("k"))
        eid = safe_int(r.get("id"))
        if k and eid is not None:
            key_to_id[k] = int(eid)

    # ───────────── stable_base -> existing n 목록(재사용용) ─────────────
    base_to_existing_ns: Dict[str, List[int]] = {}
    for k in list(key_to_id.keys()):
        if "|n:" not in k:
            continue
        base, nstr = k.rsplit("|n:", 1)
        try:
            n = int(nstr)
        except Exception:
            continue
        arr = base_to_existing_ns.get(base)
        if arr is None:
            base_to_existing_ns[base] = [n]
        else:
            arr.append(n)
    for b, arr in base_to_existing_ns.items():
        arr.sort()

    # ───────────── Second Yellow 존재 시 같은 대상/근처 Red 스킵 ─────────────
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

    # ───────────── 시간순 정렬 ─────────────
    indexed = list(enumerate(events or []))
    indexed.sort(key=lambda pair: _time_key(pair[1], pair[0]))
    evs = [ev for _, ev in indexed]

    seen_keys_in_tick: List[str] = []

    # stable_base별로 tick 안에서 몇 번째 등장인지
    base_seen_count: Dict[str, int] = {}

    # stable_base별로 이번 tick에서 사용한 n(중복 매핑 방지)
    base_used_n: Dict[str, set] = {}

    def _set_primary_key(event_id: int, primary_key: str) -> None:
        if not primary_key:
            return

        # ✅ 1) 항상 단일 UPSERT로 정리 (쿼리 수 감량)
        execute(
            """
            INSERT INTO match_event_key_map (fixture_id, canonical_key, event_id, created_at, updated_at)
            VALUES (%s, %s, %s, now(), now())
            ON CONFLICT (fixture_id, canonical_key)
            DO UPDATE SET event_id = EXCLUDED.event_id, updated_at = now()
            """,
            (fixture_id, primary_key, event_id),
        )
        key_to_id[primary_key] = int(event_id)

        # ✅ 2) event_id 당 1개 키만 유지 (단일 DELETE)
        execute(
            """
            DELETE FROM match_event_key_map
            WHERE fixture_id=%s
              AND event_id=%s
              AND canonical_key <> %s
            """,
            (fixture_id, event_id, primary_key),
        )

        # 메모리 dict도 같이 정리
        for k, vid in list(key_to_id.items()):
            if int(vid) == int(event_id) and k != primary_key:
                del key_to_id[k]

    def _build_stable_base(
        ev_type_norm: str,
        detail_norm: str,
        minute: int,
        t_id: Optional[int],
        p_id: Optional[int],
        a_id: Optional[int],
        pname: str,
        aname: str,
        player_in_id: Optional[int],
        player_in_name: Optional[str],
    ) -> str:
        mb = _minute_bucket10(minute)

        if ev_type_norm == "goal":
            kind = _goal_kind(detail_norm)
            who = f"pid:{int(p_id)}" if p_id is not None else (f"name:{pname}" if pname else "who:unk")
            return f"G|{fixture_id}|b{mb}|t{int(t_id or 0)}|{kind}|{who}"

        if ev_type_norm == "card":
            ck = _card_kind(detail_norm)
            who = f"pid:{int(p_id)}" if p_id is not None else (f"name:{pname}" if pname else "who:unk")
            return f"C|{fixture_id}|b{mb}|t{int(t_id or 0)}|{ck}|{who}"

        if ev_type_norm in ("subst", "substitution", "sub"):
            out_part = f"pid:{int(p_id)}" if p_id is not None else (f"name:{pname}" if pname else "out:unk")
            in_part = f"pid:{int(player_in_id)}" if player_in_id is not None else (
                f"name:{_safe_name(player_in_name)}" if player_in_name else "in:unk"
            )
            return f"S|{fixture_id}|b{mb}|t{int(t_id or 0)}|out:{out_part}|in:{in_part}"

        if ev_type_norm == "var":
            return f"V|{fixture_id}|b{mb}|t{int(t_id or 0)}|{detail_norm or 'var'}"

        p_part = f"pid:{int(p_id)}" if p_id is not None else (f"name:{pname}" if pname else "p:unk")
        a_part = f"pid:{int(a_id)}" if a_id is not None else (f"name:{aname}" if aname else "a:unk")
        return f"E|{fixture_id}|b{mb}|t{int(t_id or 0)}|{ev_type_norm or 'e'}|{detail_norm or 'd'}|{p_part}|{a_part}"

    for ev in evs:
        team = ev.get("team") or {}
        player = ev.get("player") or {}
        assist = ev.get("assist") or {}

        t_id = safe_int(team.get("id"))
        p_id = safe_int(player.get("id"))
        a_id = safe_int(assist.get("id"))

        ev_type = safe_text(ev.get("type"))
        detail = safe_text(ev.get("detail"))

        # bench/staff 카드 차단
        if _is_bench_staff_card(t_id, p_id, ev_type):
            continue

        tm = ev.get("time") or {}
        minute = safe_int(tm.get("elapsed"))
        extra0 = int(safe_int(tm.get("extra")) or 0)
        if minute is None:
            continue

        ev_type_norm = _norm(ev_type)
        detail_norm = _norm(detail)

        # Second Yellow가 있으면 동일키 Red 스킵
        if ev_type_norm == "card" and t_id is not None and p_id is not None:
            ksy = (int(minute), int(extra0), int(t_id), int(p_id))
            if (detail_norm == "red card") and (ksy in second_yellow_keys):
                continue

        pname = _safe_name((player.get("name") if isinstance(player, dict) else None))
        aname = _safe_name((assist.get("name") if isinstance(assist, dict) else None))

        # substitution: player=OUT / assist=IN
        player_in_id = None
        player_in_name = None
        if ev_type_norm in ("subst", "substitution", "sub"):
            player_in_id = a_id
            player_in_name = safe_text(assist.get("name"))

        incoming_id = safe_int(ev.get("id"))
        id_key = f"ID|{fixture_id}|{int(incoming_id)}" if incoming_id is not None else ""

        # ───────────── stable_base 생성(항상) ─────────────
        stable_base = _build_stable_base(
            ev_type_norm=ev_type_norm,
            detail_norm=detail_norm,
            minute=int(minute),
            t_id=t_id,
            p_id=p_id,
            a_id=a_id,
            pname=pname,
            aname=aname,
            player_in_id=player_in_id,
            player_in_name=player_in_name,
        )

        # ───────────── n(occurrence) 결정: 시간순으로 + DB 기존 n 재사용 ─────────────
        base_seen_count[stable_base] = base_seen_count.get(stable_base, 0) + 1
        desired_n = int(base_seen_count[stable_base])

        used_set = base_used_n.get(stable_base)
        if used_set is None:
            used_set = set()
            base_used_n[stable_base] = used_set

        chosen_key = ""
        chosen_event_id: Optional[int] = None

        # (1) 가장 우선: desired_n 키가 이미 있으면 그대로
        k_desired = f"{stable_base}|n:{desired_n}"
        if k_desired in key_to_id and desired_n not in used_set:
            chosen_key = k_desired
            chosen_event_id = int(key_to_id[k_desired])
            used_set.add(desired_n)

        # (2) 과거 ID|... 키가 남아있으면(예전 시스템) 그 event_id를 가져와 stable 키로 마이그레이션
        if chosen_event_id is None and id_key and (id_key in key_to_id):
            chosen_event_id = int(key_to_id[id_key])
            if desired_n not in used_set:
                chosen_key = k_desired
                used_set.add(desired_n)
            else:
                nn = desired_n
                while nn in used_set:
                    nn += 1
                chosen_key = f"{stable_base}|n:{nn}"
                used_set.add(nn)

        # (3) DB에 이미 존재하던 n들이 있으면 그 중 아직 안 쓴 n으로 매칭(순서 흔들림 완화)
        if chosen_event_id is None:
            existing_ns = base_to_existing_ns.get(stable_base) or []
            for n in existing_ns:
                if n in used_set:
                    continue
                kk = f"{stable_base}|n:{int(n)}"
                if kk in key_to_id:
                    chosen_key = kk
                    chosen_event_id = int(key_to_id[kk])
                    used_set.add(int(n))
                    break

        # (4) 여기까지 못 찾으면 새 이벤트: desired_n 키로 synthetic 생성
        if chosen_event_id is None:
            chosen_key = k_desired
            chosen_event_id = _synthetic_id_from_key(chosen_key)
            used_set.add(desired_n)

        ev_id_used = int(chosen_event_id)
        primary_key = chosen_key

        # 상태 기록(상태용)
        seen_keys_in_tick.append(primary_key)

        # ───────────── match_events UPSERT (minute/extra 보정 허용) ─────────────
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
                fixture_id = EXCLUDED.fixture_id,

                team_id = CASE
                           WHEN match_events.team_id IS NULL THEN EXCLUDED.team_id
                           ELSE match_events.team_id
                         END,

                player_id = CASE
                             WHEN match_events.player_id IS NULL THEN EXCLUDED.player_id
                             ELSE match_events.player_id
                           END,

                minute = CASE
                          WHEN match_events.minute IS NULL THEN EXCLUDED.minute
                          WHEN EXCLUDED.minute IS NULL THEN match_events.minute
                          WHEN abs(EXCLUDED.minute - match_events.minute) <= %s THEN EXCLUDED.minute
                          ELSE match_events.minute
                        END,

                extra = CASE
                         WHEN match_events.extra IS NULL THEN EXCLUDED.extra
                         WHEN EXCLUDED.extra IS NULL THEN match_events.extra
                         WHEN (match_events.minute IS NOT NULL AND EXCLUDED.minute IS NOT NULL AND abs(EXCLUDED.minute - match_events.minute) <= %s)
                              THEN EXCLUDED.extra
                         ELSE match_events.extra
                       END,

                type = CASE
                        WHEN EXCLUDED.type IS NULL OR EXCLUDED.type = '' THEN match_events.type
                        WHEN match_events.type IS NULL OR match_events.type = '' THEN EXCLUDED.type
                        ELSE match_events.type
                      END,

                detail = CASE
                          WHEN EXCLUDED.detail IS NULL OR EXCLUDED.detail = '' THEN match_events.detail
                          WHEN match_events.detail IS NULL OR match_events.detail = '' THEN EXCLUDED.detail
                          ELSE match_events.detail
                        END,

                assist_player_id = CASE
                                    WHEN match_events.assist_player_id IS NULL THEN EXCLUDED.assist_player_id
                                    ELSE match_events.assist_player_id
                                  END,
                assist_name = CASE
                               WHEN match_events.assist_name IS NULL OR match_events.assist_name = '' THEN EXCLUDED.assist_name
                               ELSE match_events.assist_name
                             END,

                player_in_id = CASE
                                WHEN match_events.player_in_id IS NULL THEN EXCLUDED.player_in_id
                                ELSE match_events.player_in_id
                              END,
                player_in_name = CASE
                                  WHEN match_events.player_in_name IS NULL OR match_events.player_in_name = '' THEN EXCLUDED.player_in_name
                                  ELSE match_events.player_in_name
                                END
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
                EVENT_MINUTE_CORR_ALLOW,
                EVENT_MINUTE_CORR_ALLOW,
            ),
        )

        # ───────────── key_map 저장 (event_id 당 1개 키만 유지) ─────────────
        _set_primary_key(ev_id_used, primary_key)

        # ───────────── 과거/기존 중복 정리 (실시간 수렴) ─────────────
        # 1) 과거에 ID|... 키가 남아있으면(예전 방식) 제거 (마이그레이션 완료)
        if id_key:
            try:
                # ✅ 중복 DELETE 제거: canonical_key 기준 단일 DELETE면 충분
                execute(
                    "DELETE FROM match_event_key_map WHERE fixture_id=%s AND canonical_key=%s",
                    (fixture_id, id_key),
                )
                key_to_id.pop(id_key, None)
            except Exception:
                pass

        # 2) incoming_id row가 따로 생긴(양수) 중복이면 제거
        if incoming_id is not None and int(incoming_id) != int(ev_id_used):
            try:
                execute(
                    "DELETE FROM match_events WHERE id=%s AND fixture_id=%s",
                    (int(incoming_id), fixture_id),
                )
            except Exception:
                pass
            try:
                execute(
                    "DELETE FROM match_event_key_map WHERE fixture_id=%s AND event_id=%s",
                    (fixture_id, int(incoming_id)),
                )
            except Exception:
                pass

    # ───────────── match_event_states 갱신 ─────────────
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
# Team stats / lineups
# ─────────────────────────────────────

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
        st = {
            "slot60": False,
            "slot10": False,
            "success": False,
            "lineups_ready": False,
            "players_by_team": {},
            "last_try_ts": 0.0,
            "inplay_tries": 0,   # ✅ INPLAY(0~15분) 라인업 재시도 횟수 제한용
        }
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
    session: requests.Session,
    fixture_id: int,
    date_utc: str,
    status_group: str,
    elapsed: Optional[int],
    now: dt.datetime,
) -> None:
    st = _ensure_lineups_state(fixture_id)

    # success + ready면 완전 잠금
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

    COOLDOWN_SEC = 20
    now_ts = time.time()
    last_try = float(st.get("last_try_ts") or 0.0)

    # UPCOMING -60 / -10 (1회성)
    if kickoff and status_group == "UPCOMING":
        mins = int((kickoff - nowu).total_seconds() / 60)

        if (59 <= mins <= 61) and not st.get("slot60"):
            st["slot60"] = True
            try:
                st["last_try_ts"] = time.time()
                resp = fetch_lineups(session, fixture_id)
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
                resp = fetch_lineups(session, fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)
                if ready:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} slot10 ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} slot10 err: {e}", file=sys.stderr)
            return

        return

    # INPLAY: 초반 재시도 (elapsed<=15)
    if status_group == "INPLAY":
        el = elapsed if elapsed is not None else 0
        if 0 <= el <= 15:
            # ✅ 과다 호출 방지: COOLDOWN + 최대 시도 횟수 상한
            tries = int(st.get("inplay_tries") or 0)
            if tries >= int(LINEUPS_INPLAY_MAX_TRIES):
                return

            if (now_ts - last_try) < COOLDOWN_SEC:
                return

            try:
                st["last_try_ts"] = time.time()
                st["inplay_tries"] = tries + 1

                resp = fetch_lineups(session, fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)
                if ready:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} inplay(el={el}) try={st['inplay_tries']} ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} inplay err: {e}", file=sys.stderr)

            return

        return

    # INPLAY: 초반 재시도 (elapsed<=15)
    if status_group == "INPLAY":
        el = elapsed if elapsed is not None else 0
        if 0 <= el <= 15:
            if (now_ts - last_try) < COOLDOWN_SEC:
                return
            try:
                st["last_try_ts"] = time.time()
                resp = fetch_lineups(session, fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, nowu)
                if ready:
                    st["success"] = True
                print(f"      [lineups] fixture_id={fixture_id} inplay(el={el}) ready={ready}")
            except Exception as e:
                print(f"      [lineups] fixture_id={fixture_id} inplay err: {e}", file=sys.stderr)


# ─────────────────────────────────────
# 이벤트 기반 스코어 보정
# ─────────────────────────────────────

def calc_score_from_events(
    events: List[Dict[str, Any]],
    home_id: int,
    away_id: int,
    hint_home_ft: Optional[int] = None,
    hint_away_ft: Optional[int] = None,
) -> Tuple[int, int]:
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

    invalid_markers = ("cancel", "disallow", "no goal", "offside", "foul", "annul", "null")
    goals: List[Dict[str, Any]] = []

    indexed = list(enumerate(events or []))
    indexed.sort(key=lambda pair: _time_key(pair[1], pair[0]))
    evs = [ev for _, ev in indexed]

    def _add_goal(ev: Dict[str, Any]) -> None:
        detail = _norm(ev.get("detail"))

        if "missed penalty" in detail:
            return
        if ("miss" in detail) and ("pen" in detail):
            return

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

    h0, a0 = _sum_scores(flip_og=False)
    h1, a1 = _sum_scores(flip_og=True)

    if hint_home_ft is not None and hint_away_ft is not None:
        d0 = abs(h0 - hint_home_ft) + abs(a0 - hint_away_ft)
        d1 = abs(h1 - hint_home_ft) + abs(a1 - hint_away_ft)
        if d1 < d0:
            return h1, a1
        return h0, a0

    return h0, a0


def update_live_score_if_needed(fixture_id: int, status_group: str, home_goals: int, away_goals: int) -> None:
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
# 메인 1회 실행
# ─────────────────────────────────────

def run_once() -> None:
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

    # fixtures 캐시
    if not hasattr(run_once, "_fixtures_cache"):
        run_once._fixtures_cache = {}  # type: ignore[attr-defined]
    fc: Dict[Tuple[int, str], Dict[str, Any]] = run_once._fixtures_cache  # type: ignore[attr-defined]

    SEASON_TTL = 60 * 60
    NOFIX_TTL = 60 * 10
    now_ts = time.time()

    for k, v in list(fc.items()):
        if float(v.get("exp") or 0) < now_ts:
            del fc[k]


    total_fixtures = 0
    total_inplay = 0
    fixture_groups: Dict[int, str] = {}

    def _update_score_any_status(fixture_id: int, home_goals: int, away_goals: int) -> None:
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

    def _postmatch_full_fetch(
        fixture_id: int,
        home_id: int,
        away_id: int,
        item: Dict[str, Any],
        tag: str,
    ) -> None:
        try:
            events = fetch_events(s, fixture_id)
            upsert_match_events_raw(fixture_id, events)
            upsert_match_events(fixture_id, events)

            goals_obj = (item.get("goals") or {})
            hint_h = safe_int(goals_obj.get("home"))
            hint_a = safe_int(goals_obj.get("away"))
            h, a = calc_score_from_events(events, home_id, away_id, hint_h, hint_a)
            _update_score_any_status(fixture_id, h, a)

            print(f"      [{tag}/events] fixture_id={fixture_id} goals={h}:{a} events={len(events)}")
        except Exception as e:
            print(f"      [{tag}/events] fixture_id={fixture_id} err: {e}", file=sys.stderr)

        try:
            stats = fetch_team_stats(s, fixture_id)
            upsert_match_team_stats(fixture_id, stats)
            LAST_STATS_SYNC[fixture_id] = time.time()
            print(f"      [{tag}/stats] fixture_id={fixture_id} updated")
        except Exception as e:
            print(f"      [{tag}/stats] fixture_id={fixture_id} err: {e}", file=sys.stderr)

        try:
            st = _ensure_lineups_state(fixture_id)
            if not st.get("lineups_ready"):
                resp = fetch_lineups(s, fixture_id)
                ready = upsert_match_lineups(fixture_id, resp, now_utc())
                if ready:
                    st["success"] = True
                print(f"      [{tag}/lineups] fixture_id={fixture_id} ready={ready}")
        except Exception as e:
            print(f"      [{tag}/lineups] fixture_id={fixture_id} err: {e}", file=sys.stderr)

    def _postmatch_already_done_in_db(fixture_id: int) -> bool:
        # ✅ 백필/수동복구로 이미 postmatch 데이터가 있으면 live worker가 다시 손대지 않게 스킵
        row = fetch_one(
            """
            SELECT
              EXISTS (SELECT 1 FROM match_team_stats WHERE fixture_id=%s) AS has_stats,
              EXISTS (SELECT 1 FROM match_events_raw  WHERE fixture_id=%s) AS has_events_raw
            """,
            (fixture_id, fixture_id),
        )
        if isinstance(row, dict):
            return bool(row.get("has_stats")) and bool(row.get("has_events_raw"))
        return False

    def _get_post_state(fixture_id: int) -> Optional[Dict[str, Any]]:
        row = fetch_one(
            """
            SELECT
              fixture_id,
              ft_seen_at,
              did_60,
              did_30m,
              last_run_at
            FROM match_postmatch_states
            WHERE fixture_id=%s
            """,
            (fixture_id,),
        )
        return row if isinstance(row, dict) else None

    def _ensure_post_state(fixture_id: int, now_dt: dt.datetime, item: Dict[str, Any]) -> Dict[str, Any]:
        st = _get_post_state(fixture_id)
        if st:
            return st

        # ✅ 새로 FINISHED를 본 케이스: ft_seen_at을 지금으로 기록
        execute(
            """
            INSERT INTO match_postmatch_states (fixture_id, ft_seen_at, did_60, did_30m, last_run_at, updated_at)
            VALUES (%s, %s, FALSE, FALSE, NULL, now())
            ON CONFLICT (fixture_id) DO NOTHING
            """,
            (fixture_id, now_dt),
        )

        # ✅ (추가) FT 최초 관측 시점에 standings 1회 갱신
        try:
            lg = item.get("league") or {}
            lid_hint = safe_int(lg.get("id"))
            season_hint = safe_int(lg.get("season"))
            refresh_standings_for_fixture(s, fixture_id, lid_hint, season_hint)
        except Exception as e:
            print(f"      [standings] fixture_id={fixture_id} refresh err: {e}", file=sys.stderr)

        st2 = _get_post_state(fixture_id)
        return st2 if st2 else {
            "fixture_id": fixture_id,
            "ft_seen_at": now_dt,
            "did_60": False,
            "did_30m": False,
            "last_run_at": None,
        }




    def _mark_post_done(fixture_id: int, *, did_60: bool, did_30m: bool) -> None:
        execute(
            """
            UPDATE match_postmatch_states
            SET did_60=%s,
                did_30m=%s,
                last_run_at=now(),
                updated_at=now()
            WHERE fixture_id=%s
            """,
            (did_60, did_30m, fixture_id),
        )

    def _schedule_postmatch_if_needed(
        fixture_id: int,
        home_id: int,
        away_id: int,
        item: Dict[str, Any],
    ) -> None:
        now_dt = now_utc()

        # ✅ 이미 백필로 복구된 경기면 postmatch 재수집 스킵 + 상태를 done으로 박아버림
        if _postmatch_already_done_in_db(fixture_id):
            st0 = _get_post_state(fixture_id)
            if not st0:
                execute(
                    """
                    INSERT INTO match_postmatch_states (fixture_id, ft_seen_at, did_60, did_30m, last_run_at, updated_at)
                    VALUES (%s, %s, TRUE, TRUE, now(), now())
                    ON CONFLICT (fixture_id) DO UPDATE SET
                      did_60=TRUE, did_30m=TRUE, last_run_at=now(), updated_at=now()
                    """,
                    (fixture_id, now_dt),
                )
            else:
                _mark_post_done(fixture_id, did_60=True, did_30m=True)

            # ✅ (추가) 백필로 이미 끝난 경기라도, FINISHED로 최초 관측되는 순간 standings는 1회 갱신
            try:
                lg = item.get("league") or {}
                lid_hint = safe_int(lg.get("id"))
                season_hint = safe_int(lg.get("season"))
                refresh_standings_for_fixture(s, fixture_id, lid_hint, season_hint)
            except Exception as e:
                print(f"      [standings] fixture_id={fixture_id} refresh err: {e}", file=sys.stderr)

            return

        # ✅ 여기서부터는 live worker가 postmatch 스케줄 관리
        st = _ensure_post_state(fixture_id, now_dt, item)

        ft_seen_at = st.get("ft_seen_at")
        if isinstance(ft_seen_at, str):
            try:
                ft_seen_at_dt = dt.datetime.fromisoformat(ft_seen_at.replace("Z", "+00:00"))
            except Exception:
                ft_seen_at_dt = now_dt
        elif isinstance(ft_seen_at, dt.datetime):
            ft_seen_at_dt = ft_seen_at
        else:
            ft_seen_at_dt = now_dt

        if ft_seen_at_dt.tzinfo is None:
            ft_seen_at_dt = ft_seen_at_dt.replace(tzinfo=dt.timezone.utc)
        else:
            ft_seen_at_dt = ft_seen_at_dt.astimezone(dt.timezone.utc)

        age_sec = (now_dt.astimezone(dt.timezone.utc) - ft_seen_at_dt).total_seconds()

        did_60 = bool(st.get("did_60"))
        did_30m = bool(st.get("did_30m"))

        if (age_sec >= 60.0) and (not did_60):
            _postmatch_full_fetch(fixture_id, home_id, away_id, item, tag="postmatch+60s")
            execute(
                """
                UPDATE match_postmatch_states
                SET did_60=TRUE, last_run_at=now(), updated_at=now()
                WHERE fixture_id=%s
                """,
                (fixture_id,),
            )
            did_60 = True

        if (age_sec >= 1800.0) and (not did_30m):
            _postmatch_full_fetch(fixture_id, home_id, away_id, item, tag="postmatch+30m")
            execute(
                """
                UPDATE match_postmatch_states
                SET did_30m=TRUE, last_run_at=now(), updated_at=now()
                WHERE fixture_id=%s
                """,
                (fixture_id,),
            )
            did_30m = True

        if did_60 or did_30m:
            execute(
                """
                UPDATE match_postmatch_states
                SET updated_at=now()
                WHERE fixture_id=%s
                """,
                (fixture_id,),
            )



    for date_str in dates:
        for lid in league_ids:
            fixtures: List[Dict[str, Any]] = []
            used_season: Optional[int] = None

            cache_key = (lid, date_str)
            cached = fc.get(cache_key)

            # ✅ (추가) fixtures 폴링 캐시: FIXTURES_POLL_INTERVAL_SEC 안에는 /fixtures 재호출 금지
            if cached and float(cached.get("exp") or 0) >= now_ts:
                if cached.get("no") is True:
                    continue

                cached_season = cached.get("season")
                cached_fx = cached.get("fixtures")
                fx_exp = float(cached.get("fx_exp") or 0.0)

                if isinstance(cached_season, int) and isinstance(cached_fx, list) and fx_exp >= now_ts:
                    fixtures = cached_fx
                    used_season = cached_season

            # 기존 캐시(시즌 고정) 기반 fetch (단, 폴링 캐시 미적중 시에만)
            if used_season is None:
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

            # ✅ (추가) 폴링 캐시 저장
            try:
                prev = fc.get(cache_key) or {}
                prev["season"] = int(used_season)
                prev["no"] = False
                prev["exp"] = float(prev.get("exp") or (now_ts + SEASON_TTL))
                prev["fx_exp"] = now_ts + float(FIXTURES_POLL_INTERVAL_SEC)
                prev["fixtures"] = fixtures
                fc[cache_key] = prev
            except Exception:
                pass

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

                    # 라인업 정책(순서 유지)
                    try:
                        elapsed = safe_int((item.get("fixture") or {}).get("status", {}).get("elapsed"))
                        maybe_sync_lineups(s, fixture_id, date_utc, sg, elapsed, now)
                    except Exception as lu_err:
                        print(f"      [lineups] fixture_id={fixture_id} policy err: {lu_err}", file=sys.stderr)

                    # FINISHED: 즉시 full fetch X, 스케줄만
                    if sg == "FINISHED":
                        _schedule_postmatch_if_needed(fixture_id, home_id, away_id, item)
                        continue

                    # INPLAY 아니면 skip
                    if sg != "INPLAY":
                        continue

                    total_inplay += 1

                    # 1) events raw 저장 + match_events 수렴 + 스코어 보정 (순서 유지)
                    try:
                        now_ts_ev = time.time()
                        last_ev = LAST_EVENTS_SYNC.get(fixture_id)
                        if (last_ev is None) or ((now_ts_ev - last_ev) >= float(EVENTS_INTERVAL_SEC)):
                            events = fetch_events(s, fixture_id)
                            LAST_EVENTS_SYNC[fixture_id] = now_ts_ev

                            upsert_match_events_raw(fixture_id, events)
                            upsert_match_events(fixture_id, events)

                            goals_obj = (item.get("goals") or {})
                            hint_h = safe_int(goals_obj.get("home"))
                            hint_a = safe_int(goals_obj.get("away"))

                            h, a = calc_score_from_events(events, home_id, away_id, hint_h, hint_a)
                            update_live_score_if_needed(fixture_id, sg, h, a)

                            print(f"      [events] fixture_id={fixture_id} goals(events)={h}:{a} events={len(events)}")
                        else:
                            # 쿨다운 중엔 events는 스킵
                            pass
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

    # 캐시 prune + dedupe prune
    finished_ids: List[int] = []
    try:
        for fid, g in list(fixture_groups.items()):
            if g in ("FINISHED", "OTHER"):
                finished_ids.append(int(fid))
                LAST_STATS_SYNC.pop(fid, None)
                LAST_EVENTS_SYNC.pop(fid, None)
                LINEUPS_STATE.pop(fid, None)

        if len(LINEUPS_STATE) > 3000:
            for fid in list(LINEUPS_STATE.keys())[: len(LINEUPS_STATE) - 2000]:
                LINEUPS_STATE.pop(fid, None)
    except Exception:
        pass

    # ✅ FINISHED에서 key_map/state를 즉시 삭제하면
    #    incoming_id 없는 이벤트가 synthetic_id로 재생성되며 "추가 삽입"이 발생할 수 있음(타임라인 중복).
    #    아래의 prune_event_dedupe_older_than(days=3)만으로 충분.

    # ✅ (변경) prune를 매틱 실행하지 말고 PRUNE_INTERVAL_SEC 주기로만 실행
    try:
        last_prune = float(getattr(run_once, "_last_prune_ts", 0.0) or 0.0)
        if (not last_prune) or ((time.time() - last_prune) >= float(PRUNE_INTERVAL_SEC)):
            prune_event_dedupe_older_than(days=3)
            run_once._last_prune_ts = time.time()  # type: ignore[attr-defined]
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

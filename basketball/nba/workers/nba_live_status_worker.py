# basketball/nba/workers/nba_live_status_worker.py
from __future__ import annotations

import os
import time
import json
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg
import sys
import faulthandler
import signal
import traceback

log = logging.getLogger("nba_live_status_worker")

# ✅ 치명 크래시(세그폴트/abort 등)도 stderr로 덤프
faulthandler.enable(all_threads=True)

# ✅ SIGTERM(렌더가 죽일 때) 잡아서 마지막 로그 남기기
def _on_term(signum, frame):
    try:
        log.error("🔥 received signal=%s; dumping traceback then exiting", signum)
        traceback.print_stack(frame)
    finally:
        raise SystemExit(1)

signal.signal(signal.SIGTERM, _on_term)
signal.signal(signal.SIGINT, _on_term)

# ✅ 잡히지 않은 예외도 무조건 남기기
def _excepthook(exc_type, exc, tb):
    try:
        log.error("🔥 unhandled exception", exc_info=(exc_type, exc, tb))
    finally:
        sys.__excepthook__(exc_type, exc, tb)

sys.excepthook = _excepthook


# ✅ 워커 단독 실행이 아니라면 basicConfig가 전체 앱 로그 설정을 덮을 수 있음.
#    루트 핸들러가 없을 때만 설정하도록 가드.
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)

BASE_URL = os.getenv("NBA_BASE", "https://v2.nba.api-sports.io").rstrip("/")

_last_schedule_scan_ts: float = 0.0


# ─────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────

def _dsn() -> str:
    dsn = (os.getenv("NBA_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        raise RuntimeError("NBA_DATABASE_URL (or DATABASE_URL) is not set")
    return dsn


def _db_fetch_one(sql: str, params: tuple = (), *, conn: Optional[psycopg.Connection] = None) -> Optional[Dict[str, Any]]:
    if conn is None:
        with psycopg.connect(_dsn()) as c:
            return _db_fetch_one(sql, params, conn=c)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        cols = [d.name for d in cur.description]
        return {cols[i]: row[i] for i in range(len(cols))}


def _db_fetch_all(sql: str, params: tuple = (), *, conn: Optional[psycopg.Connection] = None) -> List[Dict[str, Any]]:
    if conn is None:
        with psycopg.connect(_dsn()) as c:
            return _db_fetch_all(sql, params, conn=c)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
        return [{cols[i]: r[i] for i in range(len(cols))} for r in rows]


def _db_execute(sql: str, params: tuple = (), *, conn: Optional[psycopg.Connection] = None) -> None:
    if conn is None:
        with psycopg.connect(_dsn()) as c:
            _db_execute(sql, params, conn=c)
            c.commit()
            return

    with conn.cursor() as cur:
        cur.execute(sql, params)



# ─────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────

class ApiRetryableError(RuntimeError):
    def __init__(self, message: str, retry_after_sec: float = 30.0):
        super().__init__(message)
        self.retry_after_sec = float(retry_after_sec)


def _headers() -> Dict[str, str]:
    key = (os.getenv("API_KEY") or os.getenv("APISPORTS_KEY") or os.getenv("API_SPORTS_KEY") or "").strip()
    if not key:
        raise RuntimeError("API_KEY (or APISPORTS_KEY/API_SPORTS_KEY) is not set")
    return {"x-apisports-key": key}


def _get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    try:
        r = requests.get(
            url,
            headers=_headers(),
            params=params,
            timeout=45,
        )
    except requests.Timeout as e:
        # ✅ timeout은 재시도 가치가 큼
        raise ApiRetryableError(f"timeout: {url} params={params} err={e}", retry_after_sec=30.0)
    except requests.RequestException as e:
        # ✅ 네트워크류도 대체로 재시도
        raise ApiRetryableError(f"request failed: {url} params={params} err={e}", retry_after_sec=30.0)

    # ✅ HTTP 레벨 분기
    if r.status_code == 429:
        # Retry-After 헤더가 있으면 존중
        ra = r.headers.get("Retry-After")
        retry_after = 60.0
        try:
            if ra:
                retry_after = float(ra)
        except Exception:
            pass
        raise ApiRetryableError(f"rate limited(429): {url} params={params}", retry_after_sec=max(30.0, retry_after))

    if 500 <= r.status_code <= 599:
        raise ApiRetryableError(f"server error({r.status_code}): {url} params={params}", retry_after_sec=60.0)

    # 그 외는 기존대로 raise
    r.raise_for_status()

    # JSON 파싱
    try:
        data = r.json()
    except Exception as e:
        raise ApiRetryableError(f"bad json: {url} params={params} err={e}", retry_after_sec=30.0)

    # ✅ API-Sports는 HTTP 200이어도 errors로 실패할 수 있음
    errs = data.get("errors") if isinstance(data, dict) else None
    if isinstance(errs, dict) and errs:
        # errors가 레이트리밋/일시 오류일 수도 있으니 retryable로 취급
        # (치명적이면 그냥 RuntimeError로 바꿔도 됨)
        raise ApiRetryableError(f"API-Sports error: {errs}", retry_after_sec=60.0)

    return data




def _jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def _safe_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def _safe_dt(v: Any) -> Optional[dt.datetime]:
    """
    psycopg/환경에 따라 TIMESTAMPTZ가 datetime 또는 str로 올 수 있어 방어.
    """
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    return None



def _int_env(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


# ─────────────────────────────────────────
# NBA status helpers
# ─────────────────────────────────────────
# API-Sports NBA에서 status.long이 예: "Scheduled", "In Play", "Live", "Halftime", "Finished"
LIVE_STATUS_LONG = {"In Play", "Live", "Halftime"}

# ✅ 종료 / 취소 / 연기 상태를 폭넓게 인지 (API-Sports가 어떤 문자열을 쓰더라도 최소한 후보에 활용 가능)
FINISHED_STATUS_LONG = {"Finished"}
CANCELED_OR_POSTPONED_STATUS_LONG = {"Postponed", "Cancelled", "Canceled", "Suspended", "Delayed"}

NOT_STARTED_STATUS_LONG = {"Scheduled", "Time TBD"}



def _is_finished_status(status_long: str, start_utc: Optional[dt.datetime]) -> bool:
    x = (status_long or "").strip()
    if x in FINISHED_STATUS_LONG:
        return True
    # ✅ 취소/연기류는 finished로 취급하지 않음 (스케줄 재스캔으로 새 일정 반영 가능하게)
    if x in CANCELED_OR_POSTPONED_STATUS_LONG:
        return False

    # 시간 기반 fallback: "Scheduled"이 오래 유지되는 경우가 있어도 finished로 단정하지 말고 False 유지
    # (대신 candidates window에서 자연히 빠지도록 설계하는게 안전)
    return False



def _is_not_started(status_long: str) -> bool:
    return (status_long or "").strip() in NOT_STARTED_STATUS_LONG


# ─────────────────────────────────────────
# poll_state
# ─────────────────────────────────────────

def _poll_state_get_or_create(game_id: int, *, conn: psycopg.Connection) -> Dict[str, Any]:
    row = _db_fetch_one("SELECT * FROM nba_live_poll_state WHERE game_id=%s", (game_id,), conn=conn)
    if row:
        return dict(row)

    _db_execute(
        "INSERT INTO nba_live_poll_state (game_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (game_id,),
        conn=conn,
    )
    row2 = _db_fetch_one("SELECT * FROM nba_live_poll_state WHERE game_id=%s", (game_id,), conn=conn)
    return dict(row2) if row2 else {"game_id": game_id}



def _poll_state_update(game_id: int, *, conn: psycopg.Connection, **cols: Any) -> None:
    if not cols:
        return

    # ✅ 방어: 허용 컬럼만 업데이트 (실수로 잘못된 키 전달 시 워커 크래시 방지)
    allowed = {
        "pre_called_at",
        "start_called_at",
        "end_called_at",
        "post_called_at",
        "finished_at",
        "next_live_poll_at",
        "next_stats_poll_at",
        "last_standings_at",
    }

    safe_cols = {k: v for k, v in cols.items() if k in allowed}
    if not safe_cols:
        return

    keys = list(safe_cols.keys())
    sets = ", ".join([f"{k}=%s" for k in keys])
    values = [safe_cols[k] for k in keys]

    _db_execute(
        f"UPDATE nba_live_poll_state SET {sets}, updated_at=now() WHERE game_id=%s",
        tuple(values + [game_id]),
        conn=conn,
    )


def _date_range_ymd(start_utc: dt.datetime, days: int) -> List[str]:
    base = start_utc.date()
    return [(base + dt.timedelta(days=i)).isoformat() for i in range(days)]


def _api_scan_schedule_by_dates(*, dates_ymd: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in dates_ymd:
        payload = _get("/games", {"date": d})
        resp = payload.get("response") if isinstance(payload, dict) else None
        if isinstance(resp, list):
            log.info("schedule scan: date=%s items=%s", d, len(resp))
            for it in resp:
                if isinstance(it, dict):
                    out.append(it)

    return out


def _db_has_recent_postponed_or_canceled(*, conn: psycopg.Connection, days_back: int) -> bool:
    # ✅ 최근 days_back일 동안 시작 예정/시작한 경기 중 "연기/취소/딜레이" 상태가 있으면 스케줄 재스캔 트리거
    row = _db_fetch_one(
        """
        SELECT 1
        FROM nba_games
        WHERE league='standard'
          AND date_start_utc >= (now() at time zone 'utc') - (%s || ' days')::interval
          AND COALESCE(status_long,'') = ANY(%s)
        LIMIT 1
        """,
        (int(days_back), list(CANCELED_OR_POSTPONED_STATUS_LONG)),
        conn=conn,
    )
    return row is not None


# ─────────────────────────────────────────
# candidates window loader
# ─────────────────────────────────────────

def _load_live_window_game_rows(*, conn: psycopg.Connection) -> List[Dict[str, Any]]:
    """
    NBA는 league_id 개념 대신 league='standard' 중심.
    후보:
      (1) pre: now ~ now+pre_min
      (2) in-play: now - inplay_max_min ~ now + grace_min (취소/연기 제외)
      (3) 최근 종료(Finished)인데 end/post 미처리면 후보에 포함 (스탠딩/마무리 처리 목적)
    """
    pre_min = _int_env("NBA_LIVE_PRESTART_MIN", 60)
    inplay_max_min = _int_env("NBA_LIVE_INPLAY_MAX_MIN", 240)
    grace_min = _int_env("NBA_LIVE_FUTURE_GRACE_MIN", 2)
    batch_limit = _int_env("NBA_LIVE_BATCH_LIMIT", 120)

    now = _utc_now()
    upcoming_end = now + dt.timedelta(minutes=pre_min)

    inplay_start = now - dt.timedelta(minutes=inplay_max_min)
    inplay_end = now + dt.timedelta(minutes=grace_min)

    # 최근 종료 경기(예: 48시간) 중 end/post 미처리 구제 윈도우
    finished_recent_start = now - dt.timedelta(hours=48)

    rows = _db_fetch_all(
        """
        SELECT
          g.id,
          g.league,
          g.season,
          g.date_start_utc,
          g.status_long
        FROM nba_games g
        LEFT JOIN nba_live_poll_state ps
          ON ps.game_id = g.id
        WHERE g.league = 'standard'
          AND (
            -- (1) 프리/임박 구간
            (g.date_start_utc >= %s AND g.date_start_utc <= %s)

            OR

            -- (2) 라이브 후보 구간: 취소/연기 제외 (Finished 포함)
            (
              g.date_start_utc >= %s
              AND g.date_start_utc <= %s
              AND COALESCE(g.status_long,'') <> ALL(%s)
            )

            OR

            -- (3) ✅ 최근 종료(Finished) + end/post 미처리 구제
            (
              COALESCE(g.status_long,'') = 'Finished'
              AND g.date_start_utc >= %s
              AND (ps.end_called_at IS NULL OR ps.post_called_at IS NULL)
            )
          )
        ORDER BY g.date_start_utc ASC
        LIMIT %s
        """,
        (
            now, upcoming_end,
            inplay_start, inplay_end, list(CANCELED_OR_POSTPONED_STATUS_LONG),
            finished_recent_start,
            batch_limit,
        ),
        conn=conn,
    )
    return rows



# ─────────────────────────────────────────
# upsert game snapshot
# ─────────────────────────────────────────

def _api_get_game_by_id(game_id: int) -> Optional[Dict[str, Any]]:
    payload = _get("/games", {"id": int(game_id)})
    resp = payload.get("response") if isinstance(payload, dict) else None
    if isinstance(resp, list) and resp and isinstance(resp[0], dict):
        return resp[0]
    return None


def upsert_game(api_item: Dict[str, Any], *, conn: psycopg.Connection) -> Optional[Dict[str, Any]]:
    """
    nba_games에 스냅샷 반영
    return: {"id": gid, "status_long": status_long, "date_start_utc": start_utc}
    """
    gid = _safe_int(api_item.get("id"))
    if gid is None:
        return None

    # ✅ NBA는 이 워커 기준으로 league='standard'만 운영 (후보쿼리/스캔 기준도 동일)
    #    API 응답에서 league가 객체로 오는 경우가 있어 문자열화되면 후보쿼리에서 누락될 수 있으니 강제한다.
    league = "standard"
    season = _safe_int(api_item.get("season"))
    stage = _safe_int(api_item.get("stage"))


    date_obj = api_item.get("date") if isinstance(api_item.get("date"), dict) else {}
    start_str = date_obj.get("start")
    start_utc: Optional[dt.datetime] = None
    if isinstance(start_str, str) and start_str:
        try:
            start_utc = dt.datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        except Exception:
            start_utc = None

    status_obj = api_item.get("status") if isinstance(api_item.get("status"), dict) else {}
    status_long = _safe_text(status_obj.get("long"))
    if not status_long:
        status_long = _safe_text(api_item.get("status_long")) or _safe_text(api_item.get("status"))

    status_short = _safe_int(status_obj.get("short"))

    teams = api_item.get("teams") if isinstance(api_item.get("teams"), dict) else {}
    home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
    visitors = teams.get("visitors") if isinstance(teams.get("visitors"), dict) else {}

    home_team_id = _safe_int(home.get("id"))
    visitor_team_id = _safe_int(visitors.get("id"))

    arena = api_item.get("arena") if isinstance(api_item.get("arena"), dict) else {}
    arena_name = _safe_text(arena.get("name"))
    arena_city = _safe_text(arena.get("city"))
    arena_state = _safe_text(arena.get("state"))

    updated_utc = _utc_now().isoformat()

    _db_execute(
        """
        INSERT INTO nba_games (
          id,
          league, season, stage,
          status_long, status_short,
          date_start_utc,
          home_team_id, visitor_team_id,
          arena_name, arena_city, arena_state,
          raw_json,
          updated_utc
        )
        VALUES (
          %s,
          %s,%s,%s,
          %s,%s,
          %s,
          %s,%s,
          %s,%s,%s,
          %s::jsonb,
          %s
        )
        ON CONFLICT (id) DO UPDATE SET
          league = EXCLUDED.league,
          season = EXCLUDED.season,
          stage = EXCLUDED.stage,
          status_long = EXCLUDED.status_long,
          status_short = EXCLUDED.status_short,
          date_start_utc = EXCLUDED.date_start_utc,
          home_team_id = EXCLUDED.home_team_id,
          visitor_team_id = EXCLUDED.visitor_team_id,
          arena_name = EXCLUDED.arena_name,
          arena_city = EXCLUDED.arena_city,
          arena_state = EXCLUDED.arena_state,
          raw_json = EXCLUDED.raw_json,
          updated_utc = EXCLUDED.updated_utc
        """,
        (
            gid,
            league, season, stage,
            status_long, status_short,
            start_utc,
            home_team_id, visitor_team_id,
            arena_name, arena_city, arena_state,
            _jdump(api_item),
            updated_utc,
        ),
        conn=conn,
    )

    return {"id": gid, "status_long": status_long, "date_start_utc": start_utc}




def _ingest_game_stats_live(game_id: int, *, conn: psycopg.Connection) -> None:
    """
    bootstrap_nba.ingest_game_stats() 로직을 워커에 이식.
    - /games/statistics?id=GAME  -> nba_game_team_stats upsert
    - /players/statistics?game= -> nba_game_player_stats upsert
    """
    now_iso = _utc_now().isoformat()

    # (1) 팀 스탯
    d = _get("/games/statistics", {"id": int(game_id)})
    for trow in (d.get("response") or []):
        if not isinstance(trow, dict):
            continue
        team = trow.get("team") if isinstance(trow.get("team"), dict) else {}
        tid = _safe_int(team.get("id"))
        if tid is None:
            continue

        _db_execute(
            """
            INSERT INTO nba_game_team_stats (game_id, team_id, raw_json, updated_utc)
            VALUES (%s,%s,%s::jsonb,%s)
            ON CONFLICT (game_id, team_id) DO UPDATE SET
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (int(game_id), int(tid), _jdump(trow), now_iso),
            conn=conn,
        )

    # (2) 선수 스탯
    p = _get("/players/statistics", {"game": int(game_id)})
    for prow in (p.get("response") or []):
        if not isinstance(prow, dict):
            continue
        player = prow.get("player") if isinstance(prow.get("player"), dict) else {}
        pid = _safe_int(player.get("id"))
        if pid is None:
            continue

        team = prow.get("team") if isinstance(prow.get("team"), dict) else {}
        tid_i = _safe_int(team.get("id"))

        _db_execute(
            """
            INSERT INTO nba_game_player_stats (game_id, player_id, team_id, raw_json, updated_utc)
            VALUES (%s,%s,%s,%s::jsonb,%s)
            ON CONFLICT (game_id, player_id) DO UPDATE SET
              team_id=EXCLUDED.team_id,
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (
                int(game_id),
                int(pid),
                int(tid_i) if tid_i is not None else None,
                _jdump(prow),
                now_iso,
            ),
            conn=conn,
        )


def _try_ingest_game_stats(game_id: int, *, conn: psycopg.Connection) -> None:
    """
    라이브워커용 stats ingest.
    - ApiRetryableError는 상위에서 next_stats_poll_at 늘리는 용도로 활용 가능
    """
    _ingest_game_stats_live(game_id, conn=conn)

def _ingest_standings_live(*, league: str, season: int, conn: psycopg.Connection) -> int:
    """
    bootstrap_nba.ingest_standings() 로직을 워커에 이식.
    return: upsert rows count
    """
    d = _get("/standings", {"league": league, "season": int(season)})
    rows = d.get("response") or []
    if not isinstance(rows, list):
        return 0

    up = 0
    now_iso = _utc_now().isoformat()

    for r in rows:
        if not isinstance(r, dict):
            continue
        team = r.get("team") if isinstance(r.get("team"), dict) else {}
        tid = _safe_int(team.get("id"))
        if tid is None:
            continue

        conf = r.get("conference") if isinstance(r.get("conference"), dict) else {}
        div = r.get("division") if isinstance(r.get("division"), dict) else {}

        win_obj = r.get("win") if isinstance(r.get("win"), dict) else {}
        loss_obj = r.get("loss") if isinstance(r.get("loss"), dict) else {}

        _db_execute(
            """
            INSERT INTO nba_standings (
              league, season, team_id,
              conference_name, conference_rank,
              division_name, division_rank,
              win, loss, streak,
              raw_json, updated_utc
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
            ON CONFLICT (league, season, team_id) DO UPDATE SET
              conference_name=EXCLUDED.conference_name,
              conference_rank=EXCLUDED.conference_rank,
              division_name=EXCLUDED.division_name,
              division_rank=EXCLUDED.division_rank,
              win=EXCLUDED.win,
              loss=EXCLUDED.loss,
              streak=EXCLUDED.streak,
              raw_json=EXCLUDED.raw_json,
              updated_utc=EXCLUDED.updated_utc
            """,
            (
                league,
                int(season),
                int(tid),
                _safe_text(conf.get("name")),
                _safe_int(conf.get("rank")),
                _safe_text(div.get("name")),
                _safe_int(div.get("rank")),
                _safe_int(win_obj.get("total")),
                _safe_int(loss_obj.get("total")),
                _safe_int(r.get("streak")),
                _jdump(r),
                now_iso,
            ),
            conn=conn,
        )
        up += 1

    return up


def _try_ingest_standings_if_due(
    *,
    league: str,
    season: int,
    poll_state: Dict[str, Any],
    now: dt.datetime,
    conn: psycopg.Connection,
) -> int:
    """
    시즌 단위 standings는 너무 자주 호출하면 비용/레이트리밋 위험 → throttle.
    - poll_state.last_standings_at 기준
    - 기본 15분, env로 조절 가능: NBA_STANDINGS_INTERVAL_SEC
    """
    interval_sec = _float_env("NBA_STANDINGS_INTERVAL_SEC", 900.0)  # 15분

    last_at = _safe_dt(poll_state.get("last_standings_at"))
    if last_at is not None:
        try:
            if now < (last_at + dt.timedelta(seconds=float(interval_sec))):
                return 0
        except Exception:
            pass

    up = _ingest_standings_live(league=league, season=int(season), conn=conn)
    _poll_state_update(int(poll_state["game_id"]), conn=conn, last_standings_at=now)
    return up


# ─────────────────────────────────────────
# tick core (windowed)
# ─────────────────────────────────────────

def tick_once_windowed(
    rows: List[Dict[str, Any]],
    *,
    conn: psycopg.Connection,
    pre_min: int,
    post_min: int,
    live_interval_sec: float,
    stats_interval_sec: float,
) -> Tuple[int, int, int]:

    """
    하키 tick 구조를 NBA로 이식:
      - pre 1회
      - start 1회
      - live 주기 (games snapshot)
      - (옵션) stats 주기 (ingest_game_stats)
      - end 1회
      - post 1회
    """
    if not rows:
        return (0, 0, 0)

    games_upserted = 0
    stats_called = 0
    now = _utc_now()

    for r in rows:
        gid = int(r["id"])
        db_status_long = (r.get("status_long") or "").strip()
        db_start = r.get("date_start_utc")
        if isinstance(db_start, str):
            # 혹시 text로 저장된 환경 방어
            try:
                db_start = dt.datetime.fromisoformat(db_start.replace("Z", "+00:00"))
            except Exception:
                db_start = None

        # ✅ start 시간이 없거나 파싱 실패한 경우: /games?id로 한 번 채워 넣어서 유령 상태 방지
        if db_start is None:
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    res = upsert_game(api_item, conn=conn)
                    if res:
                        games_upserted += 1
                        db_status_long = (res.get("status_long") or db_status_long).strip()
                        db_start = res.get("date_start_utc") or db_start
            except Exception as e:
                log.warning("start_utc fill failed: game=%s err=%s", gid, e)


        st = _poll_state_get_or_create(gid, conn=conn)
        pre_called_at = _safe_dt(st.get("pre_called_at"))
        start_called_at = _safe_dt(st.get("start_called_at"))
        end_called_at = _safe_dt(st.get("end_called_at"))
        post_called_at = _safe_dt(st.get("post_called_at"))
        finished_at = _safe_dt(st.get("finished_at"))
        next_live_poll_at = _safe_dt(st.get("next_live_poll_at"))
        next_stats_poll_at = _safe_dt(st.get("next_stats_poll_at"))



        # (A) pre 1회
        if (
            pre_called_at is None
            and isinstance(db_start, dt.datetime)
            and (db_start - dt.timedelta(minutes=pre_min)) <= now < db_start
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    res = upsert_game(api_item, conn=conn)
                    if res:
                        games_upserted += 1
                        db_status_long = (res.get("status_long") or db_status_long).strip()
                        db_start = res.get("date_start_utc") or db_start
                    _poll_state_update(gid, conn=conn, pre_called_at=now)

            except Exception as e:
                log.warning("pre-call /games?id failed: game=%s err=%s", gid, e)
            continue

        # (B) start 1회 (now>=start & not finished)
        if (
            start_called_at is None
            and isinstance(db_start, dt.datetime)
            and now >= db_start
            and not _is_finished_status(db_status_long, db_start)
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    res = upsert_game(api_item, conn=conn)
                    if res:
                        games_upserted += 1
                        db_status_long = (res.get("status_long") or db_status_long).strip()
                        db_start = res.get("date_start_utc") or db_start
                    _poll_state_update(gid, conn=conn, start_called_at=now)
            except Exception as e:
                log.warning("start-call /games?id failed: game=%s err=%s", gid, e)

        # (C) end 1회
        if _is_finished_status(db_status_long, db_start) and end_called_at is None:
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    res = upsert_game(api_item, conn=conn)
                    if res:
                        games_upserted += 1
                        db_status_long = (res.get("status_long") or db_status_long).strip()
                        db_start = res.get("date_start_utc") or db_start

                    # ✅ 종료 시점 stats 마무리 1회
                    try:
                        _try_ingest_game_stats(gid, conn=conn)
                        stats_called += 1
                    except Exception as e:
                        log.warning("end stats failed: game=%s err=%s", gid, e)

                    # ✅ 종료 시점 standings 마무리 (throttle 적용)
                    try:
                        season_i = _safe_int(r.get("season")) or _safe_int(api_item.get("season"))
                        if season_i is not None:
                            _try_ingest_standings_if_due(
                                league="standard",
                                season=int(season_i),
                                poll_state=st,
                                now=now,
                                conn=conn,
                            )
                    except Exception as e:
                        log.warning("end standings failed: game=%s err=%s", gid, e)

                    _poll_state_update(gid, conn=conn, end_called_at=now, finished_at=now)

            except Exception as e:
                log.warning("end-call /games?id failed: game=%s err=%s", gid, e)
            continue

        # (D) post 1회 (finished + post_min)
        if (
            finished_at is not None
            and post_called_at is None
            and isinstance(finished_at, dt.datetime)
            and now >= (finished_at + dt.timedelta(minutes=post_min))
        ):
            try:
                api_item = _api_get_game_by_id(gid)
                if isinstance(api_item, dict):
                    res = upsert_game(api_item, conn=conn)
                    if res:
                        games_upserted += 1
                        db_status_long = (res.get("status_long") or db_status_long).strip()
                        db_start = res.get("date_start_utc") or db_start

                    # ✅ 종료 후 post 구간 stats 추가 1회(지연 반영 대비)
                    try:
                        _try_ingest_game_stats(gid, conn=conn)
                        stats_called += 1
                    except Exception as e:
                        log.warning("post stats failed: game=%s err=%s", gid, e)

                    # ✅ 종료 후 post standings 추가 1회(지연 반영 대비, throttle 적용)
                    try:
                        season_i = _safe_int(r.get("season")) or _safe_int(api_item.get("season"))
                        if season_i is not None:
                            _try_ingest_standings_if_due(
                                league="standard",
                                season=int(season_i),
                                poll_state=st,
                                now=now,
                                conn=conn,
                            )
                    except Exception as e:
                        log.warning("post standings failed: game=%s err=%s", gid, e)

                    _poll_state_update(gid, conn=conn, post_called_at=now)

            except Exception as e:
                log.warning("post-call /games?id failed: game=%s err=%s", gid, e)
            continue

        # (E) live periodic
        # ✅ start_called_at 이후에는 status_long이 Scheduled로 남아도(전환 지연) /games는 계속 폴링
        if (start_called_at is not None) and (not _is_finished_status(db_status_long, db_start)):
            due = False
            if next_live_poll_at is None:
                due = True
            else:
                try:
                    due = now >= next_live_poll_at
                except Exception:
                    due = True

            if due:
                # 1) /games snapshot
                try:
                    api_item = _api_get_game_by_id(gid)
                    if isinstance(api_item, dict):
                        res = upsert_game(api_item, conn=conn)
                        if res:
                            games_upserted += 1
                            db_status_long = (res.get("status_long") or db_status_long).strip()
                            db_start = res.get("date_start_utc") or db_start

                except ApiRetryableError as e:
                    log.warning("live /games?id retryable: game=%s err=%s retry_after=%.1fs", gid, e, e.retry_after_sec)
                    _poll_state_update(
                        gid,
                        conn=conn,
                        next_live_poll_at=now + dt.timedelta(seconds=max(float(e.retry_after_sec), float(live_interval_sec))),
                    )
                    continue

                except Exception as e:
                    log.warning("live /games?id failed: game=%s err=%s", gid, e)
                    _poll_state_update(
                        gid,
                        conn=conn,
                        next_live_poll_at=now + dt.timedelta(seconds=max(10.0, float(live_interval_sec))),
                    )
                    continue

                # 2) stats (DB poll_state 기반 스케줄)
                if db_status_long in LIVE_STATUS_LONG:
                    stats_due = False
                    if next_stats_poll_at is None:
                        stats_due = True
                    else:
                        try:
                            stats_due = now >= next_stats_poll_at
                        except Exception:
                            stats_due = True

                    if stats_due:
                        try:
                            _try_ingest_game_stats(gid, conn=conn)
                            stats_called += 1
                        except ApiRetryableError as e:
                            log.warning(
                                "stats retryable: game=%s err=%s retry_after=%.1fs",
                                gid, e, e.retry_after_sec
                            )
                            _poll_state_update(
                                gid,
                                conn=conn,
                                next_stats_poll_at=now + dt.timedelta(
                                    seconds=max(float(e.retry_after_sec), float(stats_interval_sec))
                                ),
                            )
                        except Exception as e:
                            log.warning("stats failed: game=%s err=%s", gid, e)
                            _poll_state_update(
                                gid,
                                conn=conn,
                                next_stats_poll_at=now + dt.timedelta(seconds=max(30.0, float(stats_interval_sec))),
                            )
                        else:
                            _poll_state_update(
                                gid,
                                conn=conn,
                                next_stats_poll_at=now + dt.timedelta(seconds=float(stats_interval_sec)),
                            )

                _poll_state_update(
                    gid,
                    conn=conn,
                    next_live_poll_at=now + dt.timedelta(seconds=float(live_interval_sec)),
                )


    return (games_upserted, stats_called, len(rows))


# ─────────────────────────────────────────
# main loop
# ─────────────────────────────────────────

def main() -> None:
    # intervals
    pre_min = _int_env("NBA_LIVE_PRESTART_MIN", 60)
    post_min = _int_env("NBA_LIVE_POSTEND_MIN", 30)

    live_interval_sec = _float_env("NBA_LIVE_INTERVAL_SEC", 10.0)
    idle_interval_sec = _float_env("NBA_LIVE_IDLE_INTERVAL_SEC", 180.0)

    # stats는 더 느리게(지금은 live_interval과 같이 호출되지만,
    # 정말 분리하려면 poll_state에 next_stats_poll_at 추가 추천)
    stats_interval_sec = _float_env("NBA_STATS_INTERVAL_SEC", 30.0)

    log.info(
        "🏀 nba live worker(start): pre=%sm post=%sm live=%.1fs idle=%.1fs stats_hint=%.1fs base=%s",
        pre_min, post_min, live_interval_sec, idle_interval_sec, stats_interval_sec, BASE_URL
    )





    global _last_schedule_scan_ts

    schedule_scan_interval_sec = _float_env("NBA_SCHEDULE_SCAN_INTERVAL_SEC", 3600.0)  # 1시간
    schedule_scan_past_days = _int_env("NBA_SCHEDULE_SCAN_PAST_DAYS", 7)
    schedule_scan_future_days = _int_env("NBA_SCHEDULE_SCAN_FUTURE_DAYS", 7)

    while True:
        try:
            with psycopg.connect(_dsn()) as conn:
                # ✅ (A) poll_state 테이블 create는 커넥션 재사용해서 수행
                _db_execute(
                    """
                    CREATE TABLE IF NOT EXISTS nba_live_poll_state (
                      game_id            INTEGER PRIMARY KEY,
                      pre_called_at      TIMESTAMPTZ,
                      start_called_at    TIMESTAMPTZ,
                      end_called_at      TIMESTAMPTZ,
                      post_called_at     TIMESTAMPTZ,
                      finished_at        TIMESTAMPTZ,
                      next_live_poll_at  TIMESTAMPTZ,
                      next_stats_poll_at TIMESTAMPTZ,
                      last_standings_at  TIMESTAMPTZ,
                      updated_at         TIMESTAMPTZ DEFAULT now()
                    );
                    """,
                    conn=conn,
                )
                conn.commit()

                # ✅ 기존 테이블이 이미 존재하는 환경(구버전)에서도 컬럼을 보장
                _db_execute("ALTER TABLE nba_live_poll_state ADD COLUMN IF NOT EXISTS next_stats_poll_at TIMESTAMPTZ;", conn=conn)
                _db_execute("ALTER TABLE nba_live_poll_state ADD COLUMN IF NOT EXISTS next_live_poll_at TIMESTAMPTZ;", conn=conn)
                _db_execute("ALTER TABLE nba_live_poll_state ADD COLUMN IF NOT EXISTS last_standings_at TIMESTAMPTZ;", conn=conn)
                _db_execute("ALTER TABLE nba_live_poll_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();", conn=conn)
                conn.commit()


                now_ts = time.time()

                # ✅ (B) 최근 연기/취소 이슈가 있거나, 주기 도래 시 스케줄 재스캔
                schedule_due = (now_ts - float(_last_schedule_scan_ts)) >= float(schedule_scan_interval_sec)
                need_rescan = False
                try:
                    if schedule_due:
                        # 최근 연기/취소가 있는지 빠르게 DB에서 체크
                        need_rescan = _db_has_recent_postponed_or_canceled(conn=conn, days_back=schedule_scan_past_days)
                except Exception as e:
                    # 체크 실패해도 스캔 자체를 막을 필요는 없음. 다만 로그만.
                    log.warning("schedule precheck failed: %s", e)

                if schedule_due or need_rescan:
                    try:
                        base_now = _utc_now()
                        # 과거 7일 + 미래 7일 날짜 목록
                        past_dates = _date_range_ymd(base_now - dt.timedelta(days=schedule_scan_past_days), schedule_scan_past_days)
                        future_dates = _date_range_ymd(base_now, schedule_scan_future_days)

                        api_items = _api_scan_schedule_by_dates(dates_ymd=(past_dates + future_dates))

                        up_cnt = 0
                        for it in api_items:
                            # ✅ API-Sports 응답에서 league 필드는 문자열/객체 등 형태가 다양할 수 있음.
                            #    스캔 단계에서 필터링하면 오히려 누락 위험이 커서 제거하고,
                            #    DB에는 upsert_game에서 league 기본값 standard로 저장되도록 둔다.
                            r = upsert_game(it, conn=conn)
                            if r:
                                up_cnt += 1


                        conn.commit()
                        _last_schedule_scan_ts = now_ts
                        log.info("schedule scan done: upserted=%s (past=%s days, future=%s days)", up_cnt, schedule_scan_past_days, schedule_scan_future_days)

                    except ApiRetryableError as e:
                        # ✅ 폭주 방지 + 너무 오래 방치 방지:
                        #    last_scan_ts를 "지금 - (interval - retry_after)" 형태로 당겨서
                        #    retry_after 이후에 다시 due가 되도록 만든다.
                        retry_after = max(30.0, float(e.retry_after_sec))
                        _last_schedule_scan_ts = now_ts - max(0.0, float(schedule_scan_interval_sec) - retry_after)
                        log.warning("schedule scan retryable: %s (retry_after=%.1fs)", e, retry_after)

                    except Exception as e:
                        _last_schedule_scan_ts = now_ts
                        log.warning("schedule scan failed: %s", e)

                # ✅ (C) 라이브 후보 로드 + tick
                rows = _load_live_window_game_rows(conn=conn)
                if not rows:
                    conn.commit()
                    time.sleep(idle_interval_sec)
                    continue

                g_up, s_up, cand = tick_once_windowed(
                    rows,
                    conn=conn,
                    pre_min=pre_min,
                    post_min=post_min,
                    live_interval_sec=live_interval_sec,
                    stats_interval_sec=stats_interval_sec,
                )

                conn.commit()
                log.info("tick done: candidates=%s games_upserted=%s stats_called=%s", cand, g_up, s_up)

            time.sleep(min(1.0, max(0.2, float(live_interval_sec) / 5.0)))

        except Exception as e:
            log.exception("tick failed: %s", e)
            time.sleep(idle_interval_sec)



if __name__ == "__main__":
    main()

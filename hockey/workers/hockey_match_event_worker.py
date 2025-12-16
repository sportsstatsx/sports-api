# hockey/workers/hockey_match_event_worker.py

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg
from psycopg_pool import ConnectionPool

# 축구 notifications/fcm_client.py 그대로 재사용
from notifications.fcm_client import FCMClient

log = logging.getLogger("hockey_match_event_worker")
logging.basicConfig(level=logging.INFO)


# ─────────────────────────────────────────
# ENV
# ─────────────────────────────────────────
def _env_str(key: str, default: str = "") -> str:
    v = os.environ.get(key)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _env_int(key: str, default: int) -> int:
    v = os.environ.get(key)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    v = os.environ.get(key)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


HOCKEY_DATABASE_URL = (
    os.environ.get("HOCKEY_DATABASE_URL")
    or os.environ.get("HOCKEY_DATABASE_URL".upper())
    or os.environ.get("hockey_database_url")
)

if not HOCKEY_DATABASE_URL:
    raise RuntimeError("HOCKEY_DATABASE_URL is not set")

# 워커 루프 주기 (초)
INTERVAL_SEC = _env_int("HOCKEY_MATCH_WORKER_INTERVAL_SEC", 10)

# 대상 리그 (쉼표 구분). 비어있으면 전체(주의: DB 부하)
LEAGUES_RAW = _env_str("HOCKEY_LIVE_LEAGUES", "")
# 후보 경기 선택 범위 (과거/미래 며칠)
PAST_DAYS = _env_int("HOCKEY_MATCH_WORKER_PAST_DAYS", 1)
FUTURE_DAYS = _env_int("HOCKEY_MATCH_WORKER_FUTURE_DAYS", 1)
# 한 tick 에 처리할 최대 경기 수
BATCH_LIMIT = _env_int("HOCKEY_MATCH_WORKER_BATCH_LIMIT", 200)
# 이벤트 알림 최대 처리 개수(과도한 스팸 방지)
MAX_EVENTS_PER_GAME_PER_TICK = _env_int("HOCKEY_MATCH_WORKER_MAX_EVENTS_PER_GAME_PER_TICK", 30)

# FCM 전송 rate 제한(너무 빠르면 부담)
SEND_SLEEP_SEC = _env_float("HOCKEY_MATCH_WORKER_SEND_SLEEP_SEC", 0.02)


def _parse_leagues(raw: str) -> List[int]:
    if not raw.strip():
        return []
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except Exception:
            continue
    return out


LEAGUE_IDS = _parse_leagues(LEAGUES_RAW)

# 하키 경기 상태(최종 종료로 간주)
FINAL_STATUSES = {
    "FT",
    "AOT",
    "AET",
    "PEN",  # 혹시
    "CANC",
    "PST",
    "ABD",
    "WO",
}

# 진행/라이브로 간주(명확히 들어오면 우선)
LIVE_STATUSES_HINT = {
    "LIVE",
    "1P",
    "2P",
    "3P",
    "OT",
    "SO",
    "P",   # pregame/paused 등 혼재 가능
}


# ─────────────────────────────────────────
# DB (하키 DB 전용)
# ─────────────────────────────────────────
pool = ConnectionPool(
    conninfo=HOCKEY_DATABASE_URL,
    kwargs={"autocommit": True},
    max_size=10,
)


def fetch_all(sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    with pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return [dict(r) for r in rows]


def fetch_one(sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
    with pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return dict(row) if row else None


def execute(sql: str, params: Optional[Sequence[Any]] = None) -> None:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())


# ─────────────────────────────────────────
# TABLES (하키 알림 전용) - 자동 생성
# ─────────────────────────────────────────
DDL = [
    """
    CREATE TABLE IF NOT EXISTS hockey_user_devices (
        device_id TEXT PRIMARY KEY,
        fcm_token TEXT NOT NULL,
        platform TEXT,
        app_version TEXT,
        timezone TEXT,
        language TEXT,
        notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS hockey_game_notification_subscriptions (
        device_id TEXT NOT NULL REFERENCES hockey_user_devices(device_id) ON DELETE CASCADE,
        game_id   INTEGER NOT NULL REFERENCES hockey_games(id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (device_id, game_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS hockey_game_notification_states (
        device_id TEXT NOT NULL REFERENCES hockey_user_devices(device_id) ON DELETE CASCADE,
        game_id   INTEGER NOT NULL REFERENCES hockey_games(id) ON DELETE CASCADE,

        last_status TEXT,
        last_home_score INTEGER NOT NULL DEFAULT 0,
        last_away_score INTEGER NOT NULL DEFAULT 0,

        last_event_id BIGINT NOT NULL DEFAULT 0,

        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

        PRIMARY KEY (device_id, game_id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_hockey_subs_game_id
    ON hockey_game_notification_subscriptions (game_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_hockey_states_game_id
    ON hockey_game_notification_states (game_id);
    """,
]


def ensure_tables() -> None:
    for stmt in DDL:
        execute(stmt)
    log.info("ensure_tables: OK")


# ─────────────────────────────────────────
# SCORE / STATUS PARSE
# ─────────────────────────────────────────
def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def parse_score(score_json: Any) -> Tuple[int, int]:
    """
    hockey_games.score_json 포맷이 리그/소스마다 조금씩 다를 수 있어서
    최대한 안전하게 home/away 합계를 뽑아냄.
    """
    if score_json is None:
        return 0, 0

    obj = score_json
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except Exception:
            return 0, 0

    if not isinstance(obj, dict):
        return 0, 0

    # 1) 가장 흔한 케이스: {"home": 2, "away": 1}
    if "home" in obj and "away" in obj and isinstance(obj.get("home"), (int, float, str)) and isinstance(obj.get("away"), (int, float, str)):
        return _to_int(obj.get("home")), _to_int(obj.get("away"))

    # 2) {"total": {"home":2, "away":1}} or {"totals": {...}}
    for k in ("total", "totals", "final", "score"):
        v = obj.get(k)
        if isinstance(v, dict) and "home" in v and "away" in v:
            return _to_int(v.get("home")), _to_int(v.get("away"))

    # 3) {"periods": {"P1":{"home":..,"away":..}, ...}, "total": ...} 없을 때 합산 시도
    periods = obj.get("periods")
    if isinstance(periods, dict):
        h = 0
        a = 0
        any_found = False
        for pv in periods.values():
            if isinstance(pv, dict) and ("home" in pv or "away" in pv):
                any_found = True
                h += _to_int(pv.get("home"))
                a += _to_int(pv.get("away"))
        if any_found:
            return h, a

    return 0, 0


def is_final_status(status: Optional[str]) -> bool:
    s = (status or "").strip().upper()
    return s in FINAL_STATUSES


def is_liveish_status(status: Optional[str]) -> bool:
    s = (status or "").strip().upper()
    if not s:
        return False
    if s in LIVE_STATUSES_HINT:
        return True
    # status_long 기반은 여기서 판단 안 함(없을 수도 있음)
    return False


# ─────────────────────────────────────────
# NOTIFICATION PAYLOAD
# ─────────────────────────────────────────
def build_title(game_row: Dict[str, Any]) -> str:
    # 팀명이 없을 수 있으니 안전하게 구성
    hid = game_row.get("home_team_id")
    aid = game_row.get("away_team_id")
    # teams 테이블 join으로 이미 이름을 가져오는 쿼리를 사용함
    home_name = str(game_row.get("home_name") or f"Home({hid})")
    away_name = str(game_row.get("away_name") or f"Away({aid})")
    return f"{home_name} vs {away_name}"


def send_push(token: str, title: str, body: str, data: Optional[Dict[str, str]] = None) -> bool:
    if not token:
        return False
    try:
        fcm = FCMClient()
        fcm.send_to_tokens(
            tokens=[token],
            title=title,
            body=body,
            data=data or {},
        )
        return True
    except Exception as e:
        log.warning("FCM send failed: %s", e)
        return False


# ─────────────────────────────────────────
# CORE LOGIC
# ─────────────────────────────────────────
@dataclass
class SubRow:
    device_id: str
    fcm_token: str
    game_id: int


def fetch_subscriptions_for_games(game_ids: List[int]) -> List[SubRow]:
    if not game_ids:
        return []
    rows = fetch_all(
        f"""
        SELECT
            s.device_id,
            d.fcm_token,
            s.game_id
        FROM hockey_game_notification_subscriptions s
        JOIN hockey_user_devices d
          ON d.device_id = s.device_id
        WHERE s.game_id = ANY(%s)
          AND COALESCE(d.notifications_enabled, TRUE) = TRUE
          AND COALESCE(d.fcm_token, '') <> ''
        """,
        (game_ids,),
    )
    out: List[SubRow] = []
    for r in rows:
        out.append(
            SubRow(
                device_id=str(r["device_id"]),
                fcm_token=str(r["fcm_token"]),
                game_id=int(r["game_id"]),
            )
        )
    return out


def load_state(device_id: str, game_id: int) -> Dict[str, Any]:
    row = fetch_one(
        """
        SELECT
            device_id,
            game_id,
            last_status,
            last_home_score,
            last_away_score,
            last_event_id
        FROM hockey_game_notification_states
        WHERE device_id = %s AND game_id = %s
        """,
        (device_id, game_id),
    )
    if row:
        return row
    # 없으면 기본 state 생성(Upsert)
    execute(
        """
        INSERT INTO hockey_game_notification_states (
            device_id, game_id, last_status, last_home_score, last_away_score, last_event_id
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (device_id, game_id) DO NOTHING
        """,
        (device_id, game_id, None, 0, 0, 0),
    )
    return {
        "device_id": device_id,
        "game_id": game_id,
        "last_status": None,
        "last_home_score": 0,
        "last_away_score": 0,
        "last_event_id": 0,
    }


def save_state(
    device_id: str,
    game_id: int,
    last_status: Optional[str],
    last_home_score: int,
    last_away_score: int,
    last_event_id: int,
) -> None:
    execute(
        """
        INSERT INTO hockey_game_notification_states (
            device_id, game_id, last_status, last_home_score, last_away_score, last_event_id, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (device_id, game_id) DO UPDATE SET
            last_status = EXCLUDED.last_status,
            last_home_score = EXCLUDED.last_home_score,
            last_away_score = EXCLUDED.last_away_score,
            last_event_id = EXCLUDED.last_event_id,
            updated_at = now()
        """,
        (device_id, game_id, last_status, last_home_score, last_away_score, last_event_id),
    )


def fetch_candidate_games(now_utc: datetime) -> List[Dict[str, Any]]:
    # window 설정
    start = now_utc.timestamp() - (PAST_DAYS * 86400)
    end = now_utc.timestamp() + (FUTURE_DAYS * 86400)

    # league 필터 동적
    league_clause = ""
    params: List[Any] = []
    params.extend([datetime.fromtimestamp(start, tz=timezone.utc), datetime.fromtimestamp(end, tz=timezone.utc)])

    if LEAGUE_IDS:
        league_clause = "AND g.league_id = ANY(%s)"
        params.append(LEAGUE_IDS)

    # 최종상태 제외 + 최근 범위만
    rows = fetch_all(
        f"""
        SELECT
            g.id,
            g.league_id,
            g.season,
            g.game_date,
            g.status,
            g.status_long,
            g.score_json,
            ht.name AS home_name,
            at.name AS away_name
        FROM hockey_games g
        LEFT JOIN hockey_teams ht ON ht.id = g.home_team_id
        LEFT JOIN hockey_teams at ON at.id = g.away_team_id
        WHERE g.game_date IS NOT NULL
          AND g.game_date >= %s
          AND g.game_date <= %s
          {league_clause}
          AND COALESCE(UPPER(g.status), '') NOT IN ({",".join(["%s"] * len(FINAL_STATUSES))})
        ORDER BY g.game_date DESC
        LIMIT {BATCH_LIMIT}
        """,
        tuple(params + list(FINAL_STATUSES)),
    )
    return rows


def fetch_new_events(game_id: int, last_event_id: int) -> List[Dict[str, Any]]:
    # id가 bigint seq라서 id 기준 증분 처리
    rows = fetch_all(
        """
        SELECT
            id,
            period,
            minute,
            team_id,
            type,
            comment,
            players,
            assists,
            event_order
        FROM hockey_game_events
        WHERE game_id = %s
          AND id > %s
        ORDER BY id ASC
        """,
        (game_id, last_event_id),
    )
    return rows


def format_event_body(game_row: Dict[str, Any], ev: Dict[str, Any], home: int, away: int) -> str:
    etype = str(ev.get("type") or "").strip().lower()
    period = str(ev.get("period") or "").strip()
    minute = ev.get("minute")
    mm = f"{minute}'" if minute is not None else ""
    prefix = " ".join([p for p in [period, mm] if p]).strip()

    # comment에 "PPG/SHG/ENG" 같은 정보가 있을 수 있음
    comment = str(ev.get("comment") or "").strip()
    if etype == "goal":
        core = "GOAL"
        if comment:
            core = f"GOAL ({comment})"
        score_part = f"{home}-{away}"
        return f"{prefix} {core}  |  {score_part}".strip()
    elif etype == "penalty":
        core = "PENALTY"
        if comment:
            core = f"PENALTY ({comment})"
        return f"{prefix} {core}".strip()
    else:
        # 기타 이벤트는 너무 스팸일 수 있어 기본은 짧게
        core = etype.upper() if etype else "EVENT"
        if comment:
            core = f"{core} ({comment})"
        return f"{prefix} {core}".strip()


def run_once() -> None:
    now_utc = datetime.now(timezone.utc)
    games = fetch_candidate_games(now_utc)

    if not games:
        log.info("tick: candidates=0")
        return

    game_ids = [int(g["id"]) for g in games]
    subs = fetch_subscriptions_for_games(game_ids)

    if not subs:
        log.info("tick: candidates=%d subs=0", len(games))
        return

    # game_id -> game row
    game_map: Dict[int, Dict[str, Any]] = {int(g["id"]): g for g in games}

    log.info("tick: candidates=%d subs=%d", len(games), len(subs))

    sent = 0
    for sub in subs:
        g = game_map.get(sub.game_id)
        if not g:
            continue

        title = build_title(g)

        # 현재 상태
        status = str(g.get("status") or "").strip()
        home, away = parse_score(g.get("score_json"))

        st = load_state(sub.device_id, sub.game_id)
        last_event_id = _to_int(st.get("last_event_id"), 0)
        last_status = st.get("last_status")
        last_home = _to_int(st.get("last_home_score"), 0)
        last_away = _to_int(st.get("last_away_score"), 0)

        # 이벤트 증분
        new_events = fetch_new_events(sub.game_id, last_event_id)

        # 너무 많으면 스팸 방지: 최신 N개만
        if len(new_events) > MAX_EVENTS_PER_GAME_PER_TICK:
            new_events = new_events[-MAX_EVENTS_PER_GAME_PER_TICK :]

        max_seen_event_id = last_event_id
        for ev in new_events:
            ev_id = _to_int(ev.get("id"), 0)
            if ev_id > max_seen_event_id:
                max_seen_event_id = ev_id

            etype = str(ev.get("type") or "").strip().lower()
            # 기본: goal/penalty만 알림
            if etype not in ("goal", "penalty"):
                continue

            body = format_event_body(g, ev, home, away)
            ok = send_push(
                token=sub.fcm_token,
                title=title,
                body=body,
                data={
                    "sport": "hockey",
                    "game_id": str(sub.game_id),
                    "type": etype,
                    "status": status,
                },
            )
            if ok:
                sent += 1
                time.sleep(SEND_SLEEP_SEC)

        # 종료 알림(상태 전환 감지)
        if is_final_status(status) and not is_final_status(str(last_status or "")):
            body = f"Final  |  {home}-{away}"
            ok = send_push(
                token=sub.fcm_token,
                title=title,
                body=body,
                data={
                    "sport": "hockey",
                    "game_id": str(sub.game_id),
                    "type": "final",
                    "status": status,
                },
            )
            if ok:
                sent += 1
                time.sleep(SEND_SLEEP_SEC)

        # 점수 변화만으로도 알림 주고 싶다면(옵션) 아래를 활성화 가능
        # if (home, away) != (last_home, last_away) and not new_events:
        #     body = f"Score Update  |  {home}-{away}"
        #     ok = send_push(sub.fcm_token, title, body, {"sport":"hockey","game_id":str(sub.game_id),"type":"score"})
        #     if ok:
        #         sent += 1
        #         time.sleep(SEND_SLEEP_SEC)

        # state 저장
        save_state(
            device_id=sub.device_id,
            game_id=sub.game_id,
            last_status=status,
            last_home_score=home,
            last_away_score=away,
            last_event_id=max_seen_event_id,
        )

    log.info("tick: sent=%d", sent)


def run_forever(interval_sec: int) -> None:
    ensure_tables()
    log.info(
        "worker start: interval=%ss leagues=%s window=%sd/%sd batch=%d",
        interval_sec,
        LEAGUE_IDS if LEAGUE_IDS else "ALL",
        PAST_DAYS,
        FUTURE_DAYS,
        BATCH_LIMIT,
    )
    while True:
        try:
            run_once()
        except Exception as e:
            log.exception("tick failed: %s", e)
        time.sleep(max(1, interval_sec))


if __name__ == "__main__":
    run_forever(INTERVAL_SEC)

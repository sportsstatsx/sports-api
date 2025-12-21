from __future__ import annotations

import json
import logging
import os
import time
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg
from psycopg_pool import ConnectionPool

# 축구 notifications/fcm_client.py 그대로 재사용
from notifications.fcm_client import FCMClient

log = logging.getLogger("hockey_match_event_worker")
logging.basicConfig(level=logging.INFO)


# ─────────────────────────────────────────
# ENV (기존 파일과 동일 키 유지)
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

# 기본(느린) 루프 주기 (초) - 기존 변수 유지
INTERVAL_SEC = _env_int("HOCKEY_MATCH_WORKER_INTERVAL_SEC", 10)

# ✅ 1부리그만 더 촘촘히 돌리고 싶을 때(옵션)
FAST_LEAGUES_RAW = _env_str("HOCKEY_MATCH_WORKER_FAST_LEAGUES", "")
FAST_INTERVAL_SEC = _env_int("HOCKEY_MATCH_WORKER_FAST_INTERVAL_SEC", 5)
SLOW_INTERVAL_SEC = _env_int("HOCKEY_MATCH_WORKER_SLOW_INTERVAL_SEC", INTERVAL_SEC)

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
FAST_LEAGUE_IDS = _parse_leagues(FAST_LEAGUES_RAW)
FAST_LEAGUE_SET = set(FAST_LEAGUE_IDS)

# 하키 경기 상태(최종 종료로 간주)
FINAL_STATUSES = {
    "FT",
    "AOT",  # After Over Time (SO 없이 OT로 끝)
    "AP",   # After Penalties (SO 종료)
    "AET",
    "PEN",
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
    "P",  # pregame/paused 등 혼재 가능
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


def table_columns(table_name: str) -> set[str]:
    """
    ✅ 안전장치:
    - 알림 워커가 hockey_game_events 스키마 변경에 발목 잡히면 안 됨.
    - 컬럼 존재 여부를 런타임에 확인하고, 존재하는 컬럼만 SELECT 한다.
    """
    rows = fetch_all(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name=%s
        """,
        (table_name,),
    )
    return set(str(r["column_name"]) for r in rows)


# ─────────────────────────────────────────
# TABLES (하키 알림 전용) - 자동 생성
#   ※ hockey_game_events 스키마는 절대 건드리지 않는다.
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

        -- ✅ 중복 알림 방지용 "발송된 이벤트 fingerprint" (문자열/해시)
        sent_event_keys TEXT[] NOT NULL DEFAULT '{}'::text[],

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

    # ✅ (2) 알림 종류 체크용 옵션 컬럼들 (기존 파일과 동일)
    execute(
        "ALTER TABLE hockey_game_notification_subscriptions "
        "ADD COLUMN IF NOT EXISTS notify_score BOOLEAN NOT NULL DEFAULT TRUE;"
    )
    execute(
        "ALTER TABLE hockey_game_notification_subscriptions "
        "ADD COLUMN IF NOT EXISTS notify_game_start BOOLEAN NOT NULL DEFAULT TRUE;"
    )
    execute(
        "ALTER TABLE hockey_game_notification_subscriptions "
        "ADD COLUMN IF NOT EXISTS notify_game_end BOOLEAN NOT NULL DEFAULT TRUE;"
    )
    execute(
        "ALTER TABLE hockey_game_notification_subscriptions "
        "ADD COLUMN IF NOT EXISTS notify_periods BOOLEAN NOT NULL DEFAULT TRUE;"
    )
    execute(
        "ALTER TABLE hockey_game_notification_subscriptions "
        "ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();"
    )

    execute(
        "ALTER TABLE hockey_game_notification_states "
        "ADD COLUMN IF NOT EXISTS sent_event_keys TEXT[] NOT NULL DEFAULT '{}'::text[];"
    )


# ─────────────────────────────────────────
# SCORE / STATUS PARSE (기존 동작 유지)
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

    # 1) {"home": 2, "away": 1}
    if (
        "home" in obj and "away" in obj
        and isinstance(obj.get("home"), (int, float, str))
        and isinstance(obj.get("away"), (int, float, str))
    ):
        return _to_int(obj.get("home")), _to_int(obj.get("away"))

    # 2) {"total": {"home":..,"away":..}} 등
    for k in ("total", "totals", "final", "score"):
        v = obj.get(k)
        if isinstance(v, dict) and "home" in v and "away" in v:
            return _to_int(v.get("home")), _to_int(v.get("away"))

    # 3) periods 합산
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
    return s in LIVE_STATUSES_HINT


def normalize_status(status: Optional[str]) -> str:
    s = (status or "").strip().upper()
    if not s:
        return ""
    if s == "P1":
        return "1P"
    if s == "P2":
        return "2P"
    if s == "P3":
        return "3P"
    return s


# ─────────────────────────────────────────
# NOTIFICATION PAYLOAD (알림 문구: 기존과 동일 유지)
# ─────────────────────────────────────────
def build_matchup(game_row: Dict[str, Any]) -> str:
    home_name = str(game_row.get("home_name") or "Home")
    away_name = str(game_row.get("away_name") or "Away")
    return f"{home_name} vs {away_name}"


def build_score_line(game_row: Dict[str, Any], home: int, away: int) -> str:
    home_name = str(game_row.get("home_name") or "Home")
    away_name = str(game_row.get("away_name") or "Away")
    # en dash(–) 유지
    return f"{home_name} {home}–{away} {away_name}"


def _period_label_from_status(status_norm: str) -> str:
    if status_norm == "1P":
        return "1st Period"
    if status_norm == "2P":
        return "2nd Period"
    if status_norm == "3P":
        return "3rd Period"
    if status_norm == "OT":
        return "Overtime"
    if status_norm == "SO":
        return "Shootout"
    return ""


def build_hockey_message(
    event_type: str,
    game_row: Dict[str, Any],
    home: int,
    away: int,
    *,
    status_norm: str = "",
    period: str = "",
    minute: Any = None,
    team_name: str = "",
    tag: str = "",
) -> Tuple[str, str]:
    matchup = build_matchup(game_row)
    score_line = build_score_line(game_row, home, away)

    if event_type == "game_start":
        return ("▶ Game Started", matchup)

    if event_type == "period_start":
        label = _period_label_from_status(status_norm) or "Period"
        return (f"▶ {label} Start", score_line)

    if event_type == "period_end":
        label = _period_label_from_status(status_norm) or "Period"
        return (f"⏸ {label} End", score_line)

    if event_type == "ot_start":
        return ("▶ Overtime", score_line)

    if event_type == "so_start":
        return ("🥅 Shootout", score_line)

    if event_type == "ot_end":
        return ("⏱ End of OT", score_line)

    if event_type == "final":
        return ("⏱ Final", score_line)

    mm = ""
    try:
        if minute is not None and str(minute).strip() != "":
            mm = f"{int(minute)}'"
    except Exception:
        mm = ""
    time_prefix = " ".join([p for p in [period.strip(), mm] if p]).strip()

    if event_type == "goal":
        who = team_name.strip() or "Goal"

        tag_norm = (tag or "").strip().upper()
        tag_line = ""
        if tag_norm == "PPG":
            tag_line = "Power-play Goal!"
        elif tag_norm == "SHG":
            tag_line = "Short-handed Goal!"
        elif tag_norm == "ENG":
            tag_line = "Empty-net Goal!"

        if time_prefix:
            title = f"🏒 {time_prefix} {who} Goal!"
        else:
            title = f"🏒 {who} Goal!"

        body = score_line if not tag_line else f"{tag_line}\n{score_line}"
        return (title, body)

    if event_type == "penalty":
        who = team_name.strip()
        who_part = f"{who} " if who else ""
        if time_prefix:
            return (f"⛔ {time_prefix} {who_part}Penalty", score_line)
        return (f"⛔ {who_part}Penalty", score_line)

    return ("Match update", score_line)


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
    notify_score: bool
    notify_game_start: bool
    notify_game_end: bool
    notify_periods: bool


def fetch_subscriptions_for_games(game_ids: List[int]) -> List[SubRow]:
    if not game_ids:
        return []
    rows = fetch_all(
        """
        SELECT
            s.device_id,
            d.fcm_token,
            s.game_id,
            COALESCE(s.notify_score, TRUE) AS notify_score,
            COALESCE(s.notify_game_start, TRUE) AS notify_game_start,
            COALESCE(s.notify_game_end, TRUE) AS notify_game_end,
            COALESCE(s.notify_periods, TRUE) AS notify_periods
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
                notify_score=bool(r["notify_score"]),
                notify_game_start=bool(r["notify_game_start"]),
                notify_game_end=bool(r["notify_game_end"]),
                notify_periods=bool(r["notify_periods"]),
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
            last_event_id,
            sent_event_keys
        FROM hockey_game_notification_states
        WHERE device_id = %s AND game_id = %s
        """,
        (device_id, game_id),
    )

    if row:
        return row

    execute(
        """
        INSERT INTO hockey_game_notification_states (
            device_id, game_id, last_status, last_home_score, last_away_score, last_event_id, sent_event_keys
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (device_id, game_id) DO NOTHING
        """,
        (device_id, game_id, None, 0, 0, 0, []),
    )
    return {
        "device_id": device_id,
        "game_id": game_id,
        "last_status": None,
        "last_home_score": 0,
        "last_away_score": 0,
        "last_event_id": 0,
        "sent_event_keys": [],
    }


def save_state(
    device_id: str,
    game_id: int,
    last_status: Optional[str],
    last_home_score: int,
    last_away_score: int,
    last_event_id: int,
    sent_event_keys: List[str],
) -> None:
    execute(
        """
        INSERT INTO hockey_game_notification_states (
            device_id, game_id, last_status, last_home_score, last_away_score, last_event_id, sent_event_keys, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (device_id, game_id) DO UPDATE SET
            last_status = EXCLUDED.last_status,
            last_home_score = EXCLUDED.last_home_score,
            last_away_score = EXCLUDED.last_away_score,
            last_event_id = EXCLUDED.last_event_id,
            sent_event_keys = EXCLUDED.sent_event_keys,
            updated_at = now()
        """,
        (device_id, game_id, last_status, last_home_score, last_away_score, last_event_id, sent_event_keys),
    )


def fetch_candidate_games(now_utc: datetime) -> List[Dict[str, Any]]:
    start = now_utc.timestamp() - (PAST_DAYS * 86400)
    end = now_utc.timestamp() + (FUTURE_DAYS * 86400)

    league_clause = ""
    params: List[Any] = [
        datetime.fromtimestamp(start, tz=timezone.utc),
        datetime.fromtimestamp(end, tz=timezone.utc),
    ]

    if LEAGUE_IDS:
        league_clause = "AND g.league_id = ANY(%s)"
        params.append(LEAGUE_IDS)

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
            g.home_team_id,
            g.away_team_id,
            ht.name AS home_name,
            at.name AS away_name
        FROM hockey_games g
        LEFT JOIN hockey_teams ht ON ht.id = g.home_team_id
        LEFT JOIN hockey_teams at ON at.id = g.away_team_id
        WHERE g.game_date IS NOT NULL
          AND g.game_date >= %s
          AND g.game_date <= %s
          {league_clause}
          AND (
            COALESCE(UPPER(g.status), '') NOT IN ({",".join(["%s"] * len(FINAL_STATUSES))})
            OR g.updated_at >= NOW() - interval '6 hours'
          )
        ORDER BY g.game_date DESC
        LIMIT {BATCH_LIMIT}
        """,
        tuple(params + list(FINAL_STATUSES)),
    )
    return rows


def _normalize_players(val: Any) -> List[str]:
    """
    hockey_game_events.players/assists 타입이 흔들려도 안전하게 리스트[str]로 변환
    """
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x) for x in val if str(x).strip()]
    if isinstance(val, tuple):
        return [str(x) for x in val if str(x).strip()]
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        try:
            obj = json.loads(s)
            if isinstance(obj, list):
                return [str(x) for x in obj if str(x).strip()]
        except Exception:
            pass
        return [s]
    return []


def event_persist_key(ev: Dict[str, Any]) -> str:
    """
    ✅ '빈 골 → 업데이트로 상세 채워짐' 케이스에서 중복 알림을 막기 위한 고정 키

    핵심:
    - players/assists/comment 같은 "나중에 채워지는 필드"는 디듀프 키에 넣지 않는다.
    - 대신 같은 골을 대표할 수 있는 "안정적인 식별자"를 쓴다.
      1) event_order 가 있으면 그걸 사용 (리그/데이터에서 순번 역할)
      2) 없으면 DB row id 를 사용 (같은 row 업데이트면 id 동일)

    결과:
    - 같은 골이 UPDATE 되어도 key가 변하지 않아서 "두 번째 알림"은 스킵된다.
    - 같은 분에 2골이 나와도 event_order/id가 달라서 스킵되지 않는다.
    """
    period = str(ev.get("period") or "").strip()
    minute = str(ev.get("minute") or "").strip()
    team_id = str(ev.get("team_id") or "").strip()
    etype = str(ev.get("type") or "").strip().lower()

    # 안정 식별자 우선순위: event_order > id
    order_val = ev.get("event_order")
    order_key = str(order_val).strip() if order_val is not None else ""
    if not order_key:
        order_key = str(_to_int(ev.get("id"), 0))

    return f"{etype}|{period}|{minute}|{team_id}|{order_key}"



def _hash_key(s: str) -> str:
    raw = (s or "").encode("utf-8", errors="ignore")
    return "h1:" + hashlib.sha1(raw).hexdigest()


def fetch_new_events(game_id: int, last_event_id: int, events_cols: set[str]) -> List[Dict[str, Any]]:
    """
    ✅ 절대 event_key/notif_key 컬럼에 의존하지 않는다.
    존재하는 컬럼만 SELECT해서, 스키마 변경이 있어도 워커가 DB를 계속 때리며 터지지 않게 한다.
    """
    cols = ["id", "period", "minute", "team_id", "type", "comment", "updated_at"]

    if "players" in events_cols:
        cols.append("players")
    if "assists" in events_cols:
        cols.append("assists")
    if "event_order" in events_cols:
        cols.append("event_order")
    if "raw_json" in events_cols:
        cols.append("raw_json")

    select_sql = ",\n            ".join(cols)

    rows = fetch_all(
        f"""
        SELECT
            {select_sql}
        FROM hockey_game_events
        WHERE game_id = %s
          AND (
            id > %s
            OR updated_at >= NOW() - interval '180 seconds'
          )
        ORDER BY id ASC
        """,
        (game_id, last_event_id),
    )
    return rows


def run_once(events_cols: set[str]) -> bool:
    """
    returns:
      - True  => fast interval recommended
      - False => slow interval recommended
    """
    now_utc = datetime.now(timezone.utc)
    games = fetch_candidate_games(now_utc)

    if not games:
        log.info("tick: candidates=0")
        return False

    # ✅ (4) 워커 동작 조건: 기존 로직 유지 (now 기준 ±6시간 & fast league면 fast)
    now_ts = now_utc.timestamp()
    has_fast_candidate = False
    if FAST_LEAGUE_SET:
        for g in games:
            try:
                lg = int(g.get("league_id") or 0)
            except Exception:
                lg = 0
            if lg not in FAST_LEAGUE_SET:
                continue

            gd = g.get("game_date")
            gd_ts = gd.timestamp() if isinstance(gd, datetime) else None
            if gd_ts is None:
                continue

            if (now_ts - 6 * 3600) <= gd_ts <= (now_ts + 6 * 3600):
                has_fast_candidate = True
                break

    game_ids = [int(g["id"]) for g in games]
    subs = fetch_subscriptions_for_games(game_ids)

    if not subs:
        log.info("tick: candidates=%d subs=0", len(games))
        return has_fast_candidate

    game_map: Dict[int, Dict[str, Any]] = {int(g["id"]): g for g in games}
    log.info("tick: candidates=%d subs=%d", len(games), len(subs))

    sent = 0

    for sub in subs:
        g = game_map.get(sub.game_id)
        if not g:
            continue

        status = str(g.get("status") or "").strip()
        home, away = parse_score(g.get("score_json"))

        st = load_state(sub.device_id, sub.game_id)
        last_event_id = _to_int(st.get("last_event_id"), 0)
        last_status = st.get("last_status")
        last_home = _to_int(st.get("last_home_score"), 0)
        last_away = _to_int(st.get("last_away_score"), 0)

        work_last_home = last_home
        work_last_away = last_away

        sent_hist = st.get("sent_event_keys") or []
        if not isinstance(sent_hist, list):
            sent_hist = []
        sent_hist_set = set(str(x) for x in sent_hist if str(x))

        last_status_norm = normalize_status(last_status)
        status_norm = normalize_status(status)

        def _send_status_notif(ntype: str, title: str, body: str) -> None:
            nonlocal sent
            sk = f"status:{ntype}"
            if sk in sent_hist_set:
                return

            ok = send_push(
                token=sub.fcm_token,
                title=title,
                body=body,
                data={
                    "sport": "hockey",
                    "game_id": str(sub.game_id),
                    "type": ntype,
                    "status": status,
                },
            )
            if ok:
                sent_hist_set.add(sk)
                sent_hist.append(sk)
                sent += 1
                time.sleep(SEND_SLEEP_SEC)

        # ─────────────────────────────
        # (A) 상태 전환 알림: 기존 조건 유지
        # ─────────────────────────────
        if sub.notify_game_start and (status_norm == "1P") and (last_status_norm != "1P"):
            t, b = build_hockey_message("game_start", g, home, away)
            _send_status_notif("game_start", t, b)

        if sub.notify_periods and (last_status_norm == "1P") and (status_norm == "BT"):
            t, b = build_hockey_message("period_end", g, home, away, status_norm="1P")
            _send_status_notif("period_end_1", t, b)

        if sub.notify_periods and (last_status_norm == "BT") and (status_norm == "2P"):
            t, b = build_hockey_message("period_start", g, home, away, status_norm="2P")
            _send_status_notif("period_start_2", t, b)

        if sub.notify_periods and (last_status_norm == "2P") and (status_norm == "BT"):
            t, b = build_hockey_message("period_end", g, home, away, status_norm="2P")
            _send_status_notif("period_end_2", t, b)

        if sub.notify_periods and (last_status_norm == "BT") and (status_norm == "3P"):
            t, b = build_hockey_message("period_start", g, home, away, status_norm="3P")
            _send_status_notif("period_start_3", t, b)

        if sub.notify_periods and (last_status_norm == "3P") and (status_norm == "OT"):
            t, b = build_hockey_message("ot_start", g, home, away)
            _send_status_notif("ot_start", t, b)

        if sub.notify_periods and (last_status_norm == "OT") and (status_norm == "SO"):
            t, b = build_hockey_message("so_start", g, home, away)
            _send_status_notif("so_start", t, b)

        if sub.notify_periods and (last_status_norm == "OT") and (status_norm in ("SO",)):
            t, b = build_hockey_message("ot_end", g, home, away)
            _send_status_notif("ot_end", t, b)

        if sub.notify_periods and (last_status_norm == "OT") and is_final_status(status_norm):
            t, b = build_hockey_message("ot_end", g, home, away)
            _send_status_notif("ot_end", t, b)

        if sub.notify_game_end and is_final_status(status_norm) and (not is_final_status(last_status_norm)):
            t, b = build_hockey_message("final", g, home, away)
            _send_status_notif("final", t, b)

        # ─────────────────────────────
        # (B) 이벤트 알림: goal만 + 스키마 의존 없는 디듀프
        # ─────────────────────────────
        new_events = fetch_new_events(sub.game_id, last_event_id, events_cols)

        if len(new_events) > MAX_EVENTS_PER_GAME_PER_TICK:
            new_events = new_events[-MAX_EVENTS_PER_GAME_PER_TICK :]

        max_seen_event_id = last_event_id
        sent_keys_tick: set[str] = set()

        for ev in new_events:
            ev_id = _to_int(ev.get("id"), 0)
            if ev_id > max_seen_event_id:
                max_seen_event_id = ev_id

            etype = str(ev.get("type") or "").strip().lower()

            # ✅ (3) goal만인지: goal 외는 전부 스킵
            if etype != "goal":
                continue

            # ✅ (2) 알림 종류 체크: notify_score 꺼져 있으면 goal도 스킵
            if not sub.notify_score:
                continue

            # ✅ 스키마 의존 없는 키
            nk = event_persist_key(ev)
            persist_key = _hash_key(f"{sub.game_id}:{nk}")

            # tick 내 디듀프
            if persist_key in sent_keys_tick:
                continue
            sent_keys_tick.add(persist_key)

            # 영속 디듀프
            if persist_key in sent_hist_set:
                continue

            ev_team_id = _to_int(ev.get("team_id"), 0)
            home_team_id = _to_int(g.get("home_team_id"), 0)
            away_team_id = _to_int(g.get("away_team_id"), 0)

            home_name = str(g.get("home_name") or "Home")
            away_name = str(g.get("away_name") or "Away")

            team_name = ""
            if ev_team_id and home_team_id and ev_team_id == home_team_id:
                team_name = home_name
            elif ev_team_id and away_team_id and ev_team_id == away_team_id:
                team_name = away_name

            period = str(ev.get("period") or "").strip()
            minute = ev.get("minute")
            tag = str(ev.get("comment") or "").strip()

            # ✅ 정책 유지: 알림 점수는 score_json과 동일
            notif_home = home
            notif_away = away
            work_last_home = home
            work_last_away = away

            t, b = build_hockey_message(
                "goal",
                g,
                notif_home,
                notif_away,
                period=period,
                minute=minute,
                team_name=team_name,
                tag=tag,
            )

            ok = send_push(
                token=sub.fcm_token,
                title=t,
                body=b,
                data={
                    "sport": "hockey",
                    "game_id": str(sub.game_id),
                    "type": etype,
                    "status": status,
                },
            )
            if ok:
                sent_hist_set.add(persist_key)
                sent_hist.append(persist_key)
                sent += 1
                time.sleep(SEND_SLEEP_SEC)

        # hist 폭주 방지: 최근 200개만
        if len(sent_hist) > 200:
            sent_hist = sent_hist[-200:]

        save_state(
            device_id=sub.device_id,
            game_id=sub.game_id,
            last_status=status,
            last_home_score=work_last_home,
            last_away_score=work_last_away,
            last_event_id=max_seen_event_id,
            sent_event_keys=sent_hist,
        )

    log.info("tick: sent=%d", sent)
    return has_fast_candidate


def run_forever(interval_sec: int) -> None:
    ensure_tables()
    events_cols = table_columns("hockey_game_events")

    log.info(
        "worker start: interval=%ss leagues=%s window=%sd/%sd batch=%d fast_leagues=%s fast=%ss slow=%ss",
        interval_sec,
        LEAGUE_IDS if LEAGUE_IDS else "ALL",
        PAST_DAYS,
        FUTURE_DAYS,
        BATCH_LIMIT,
        FAST_LEAGUE_IDS if FAST_LEAGUE_IDS else "NONE",
        FAST_INTERVAL_SEC,
        SLOW_INTERVAL_SEC,
    )

    while True:
        use_fast = False
        try:
            use_fast = run_once(events_cols)
        except Exception as e:
            log.exception("tick failed: %s", e)

        if FAST_LEAGUE_SET and use_fast:
            sleep_sec = max(1, FAST_INTERVAL_SEC)
        else:
            sleep_sec = max(1, SLOW_INTERVAL_SEC)

        time.sleep(sleep_sec)


if __name__ == "__main__":
    run_forever(INTERVAL_SEC)

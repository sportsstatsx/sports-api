from __future__ import annotations

import json
import logging
import os
import time
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
# ENV
# ─────────────────────────────────────────
def _env_str(key: str, default: str = "") -> str:
    v = os.environ.get(key)
    if v is None:
        v = os.environ.get(key.upper())
    if v is None:
        v = os.environ.get(key.lower())
    return str(v).strip() if v is not None else default


def _env_int(key: str, default: int) -> int:
    try:
        return int(float(_env_str(key, str(default)) or default))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(_env_str(key, str(default)) or default)
    except Exception:
        return default


def _env_int_list(key: str) -> List[int]:
    raw = _env_str(key, "")
    if not raw:
        return []
    out: List[int] = []
    for x in raw.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.append(int(x))
        except Exception:
            continue
    return out


DATABASE_URL = _env_str("HOCKEY_DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError("HOCKEY_DATABASE_URL is not set")

# interval은 run_forever에서 로그용으로만 씀(실제 sleep은 FAST/SLOW)
INTERVAL_SEC = _env_int("HOCKEY_NOTIF_INTERVAL_SEC", 10)

# 후보 경기 window
PAST_DAYS = _env_int("HOCKEY_NOTIF_PAST_DAYS", 1)
FUTURE_DAYS = _env_int("HOCKEY_NOTIF_FUTURE_DAYS", 1)

# 후보 경기 리그 제한
LEAGUE_IDS = _env_int_list("HOCKEY_NOTIF_LEAGUE_IDS")
LEAGUE_SET = set(LEAGUE_IDS)

# 구독 가져올 때 batch 제한
BATCH_LIMIT = _env_int("HOCKEY_NOTIF_BATCH_LIMIT", 250)

# send sleep
SEND_SLEEP_SEC = _env_float("HOCKEY_NOTIF_SEND_SLEEP_SEC", 0.1)

# fast/slow interval (기존 유지)
FAST_INTERVAL_SEC = _env_int("HOCKEY_NOTIF_FAST_INTERVAL_SEC", 2)
SLOW_INTERVAL_SEC = _env_int("HOCKEY_NOTIF_SLOW_INTERVAL_SEC", 10)
FAST_LEAGUE_IDS = _env_int_list("HOCKEY_NOTIF_FAST_LEAGUE_IDS")
FAST_LEAGUE_SET = set(FAST_LEAGUE_IDS)

# ─────────────────────────────────────────
# DB POOL
# ─────────────────────────────────────────
_pool = ConnectionPool(conninfo=DATABASE_URL, open=True)


def execute(sql: str, params: Tuple[Any, ...] = ()) -> int:
    with _pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


def fetch_one(sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    with _pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def fetch_all(sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    with _pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows] if rows else []


# ─────────────────────────────────────────
# TABLES
# ─────────────────────────────────────────
def ensure_tables() -> None:
    """
    기존 테이블/컬럼 유지.
    FSM 리팩토링 후에도 subscriptions/states 테이블은 그대로 사용.
    """
    execute(
        """
        CREATE TABLE IF NOT EXISTS hockey_game_notification_subscriptions (
          device_id TEXT NOT NULL,
          game_id INTEGER NOT NULL,
          notify_score BOOLEAN NOT NULL DEFAULT TRUE,
          notify_game_start BOOLEAN NOT NULL DEFAULT TRUE,
          notify_game_end BOOLEAN NOT NULL DEFAULT TRUE,
          notify_periods BOOLEAN NOT NULL DEFAULT TRUE,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (device_id, game_id)
        );
        """
    )

    execute(
        """
        CREATE TABLE IF NOT EXISTS hockey_game_notification_states (
          device_id TEXT NOT NULL,
          game_id INTEGER NOT NULL,
          last_status TEXT,
          last_home_score INTEGER NOT NULL DEFAULT 0,
          last_away_score INTEGER NOT NULL DEFAULT 0,
          last_event_id INTEGER NOT NULL DEFAULT 0,
          sent_event_keys TEXT[] NOT NULL DEFAULT '{}'::text[],
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (device_id, game_id)
        );
        """
    )

    # 기존 컬럼 보강(있으면 무시)
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


FINAL_STATUSES = {
    # regulation / generic final
    "FT", "FINAL", "FINISHED",
    # overtime finished (API-Sports에서 종종 AOT로 옴)
    "AOT",
    # after penalties/shootout (API-Sports: AP)
    "AP",
    # keep legacy tokens (혹시 다른 소스에서 올 수 있음)
    "AET", "PEN",
}
LIVE_STATUSES_HINT = {
    "P1", "P2", "P3", "OT", "SO", "LIVE",
}

def extract_ui_scores(score_json: Any, raw_json: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    UI(/api/hockey/fixtures)와 동일한 스코어 산출 규칙

    1) score_json['home'/'away']
    2) 없으면 raw_json['scores']['home'/'away']
    3) 둘 다 없으면 (None, None)
    """

    def _safe_int(x: Any) -> Optional[int]:
        try:
            if x is None or isinstance(x, bool):
                return None
            if isinstance(x, (int, float)):
                return int(x)
            s = str(x).strip()
            if not s:
                return None
            return int(float(s))
        except Exception:
            return None

    # 1) score_json 우선
    sj = score_json
    if isinstance(sj, str):
        try:
            sj = json.loads(sj)
        except Exception:
            sj = None

    if isinstance(sj, dict):
        h = _safe_int(sj.get("home"))
        a = _safe_int(sj.get("away"))
        if h is not None or a is not None:
            return h, a

    # 2) raw_json fallback
    rj = raw_json
    if isinstance(rj, str):
        try:
            rj = json.loads(rj)
        except Exception:
            rj = None

    if isinstance(rj, dict):
        scores = rj.get("scores")
        if isinstance(scores, dict):
            h = _safe_int(scores.get("home"))
            a = _safe_int(scores.get("away"))
            if h is not None or a is not None:
                return h, a

    return None, None

def extract_db_scores_only(score_json: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    알림용 canonical score:
    - hockey_games.score_json만 사용
    - raw_json fallback 사용 금지
    - 값이 없으면 None 반환 -> caller에서 last score 유지
    """
    def _safe_int(x: Any) -> Optional[int]:
        try:
            if x is None or isinstance(x, bool):
                return None
            if isinstance(x, (int, float)):
                return int(x)
            s = str(x).strip()
            if not s:
                return None
            return int(float(s))
        except Exception:
            return None

    sj = score_json
    if isinstance(sj, str):
        try:
            sj = json.loads(sj)
        except Exception:
            sj = None

    if not isinstance(sj, dict):
        return None, None

    # 1) 가장 우선: direct home/away
    h = _safe_int(sj.get("home"))
    a = _safe_int(sj.get("away"))
    if h is not None or a is not None:
        return h, a

    # 2) nested total/final/score
    for k in ("total", "totals", "final", "score"):
        v = sj.get(k)
        if isinstance(v, dict):
            hh = _safe_int(v.get("home"))
            aa = _safe_int(v.get("away"))
            if hh is not None or aa is not None:
                return hh, aa

    return None, None

def is_final_status(status: Optional[str]) -> bool:
    s = (status or "").strip().upper()
    return s in FINAL_STATUSES



def normalize_status(status: Any) -> str:
    s = str(status or "").strip().upper()
    if not s:
        return ""
    # API-Sports 스타일도 흡수
    # P1/P2/P3/OT/SO/NS/FT/BT + AP(Af. Penalties) + AOT(After OT)
    if s in ("NS", "TBD"):
        return "NS"
    if s in ("P1", "1P"):
        return "1P"
    if s in ("P2", "2P"):
        return "2P"
    if s in ("P3", "3P"):
        return "3P"
    if s in ("OT",):
        return "OT"
    if s in ("SO",):
        return "SO"
    if s in ("BT", "BREAK", "INTERMISSION"):
        return "BT"

    # 종료 계열
    if s in ("AP",):         # After Penalties (API-Sports)
        return "AP"
    if s in ("AOT",):        # After Over Time (API-Sports)
        return "AOT"
    if s in ("FT", "FINAL", "FINISHED"):
        return "FT"

    return s

SCORE_NOTIFY_LIVE_STATUSES = {"1P", "2P", "3P", "OT", "SO"}

def is_score_notify_live_status(status_norm: str) -> bool:
    return status_norm in SCORE_NOTIFY_LIVE_STATUSES

def _is_subscription_newer_than_state(
    subscribed_at: Any,
    state_updated_at: Any,
) -> bool:
    """
    구독(updated_at)이 state(updated_at)보다 더 최신이면
    '재구독/재설정 이후'로 보고 baseline 재시드해야 함.
    """
    if subscribed_at is None:
        return False
    if state_updated_at is None:
        return True

    if isinstance(subscribed_at, datetime) and isinstance(state_updated_at, datetime):
        return subscribed_at > state_updated_at

    try:
        return str(subscribed_at) > str(state_updated_at)
    except Exception:
        return False

# ─────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────
@dataclass
class Subscription:
    device_id: str
    fcm_token: str
    game_id: int
    notify_score: bool
    notify_game_start: bool
    notify_game_end: bool
    notify_periods: bool


# ─────────────────────────────────────────
# STATE
# ─────────────────────────────────────────
def load_state(device_id: str, game_id: int) -> Dict[str, Any]:
    row = fetch_one(
        """
        SELECT
          last_status,
          last_home_score,
          last_away_score,
          sent_event_keys,
          updated_at
        FROM hockey_game_notification_states
        WHERE device_id=%s AND game_id=%s
        """,
        (device_id, game_id),
    )

    if row:
        row["_exists"] = True
        return row

    return {
        "_exists": False,
        "last_status": None,
        "last_home_score": 0,
        "last_away_score": 0,
        "sent_event_keys": [],
        "updated_at": None,
    }



def save_state(
    device_id: str,
    game_id: int,
    last_status: Optional[str],
    last_home_score: int,
    last_away_score: int,
    sent_event_keys: List[str],
) -> None:
    execute(
        """
        INSERT INTO hockey_game_notification_states
          (device_id, game_id, last_status, last_home_score, last_away_score, sent_event_keys, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (device_id, game_id)
        DO UPDATE SET
          last_status = EXCLUDED.last_status,
          last_home_score = EXCLUDED.last_home_score,
          last_away_score = EXCLUDED.last_away_score,
          sent_event_keys = EXCLUDED.sent_event_keys,
          updated_at = now()
        """,
        (
            device_id,
            game_id,
            last_status,
            last_home_score,
            last_away_score,
            sent_event_keys,
        ),
    )

def fetch_last_goal_minute(game_id: int) -> Optional[str]:
    """
    가장 최근 goal 이벤트의 minute(분)만 조회
    예: 11"
    - hockey_game_events 스키마 기준: type, minute, created_at
    """
    row = fetch_one(
        """
        SELECT minute
        FROM hockey_game_events
        WHERE game_id = %s
          AND type = 'goal'
          AND minute IS NOT NULL
        ORDER BY created_at DESC, event_order DESC, id DESC
        LIMIT 1
        """,
        (game_id,),
    )

    if not row:
        return None

    m = row.get("minute")
    if m is None:
        return None

    # minute은 smallint라서 그냥 int 변환만 하면 됨
    try:
        mi = int(m)
    except Exception:
        s = str(m).strip()
        if not s:
            return None
        if ":" in s:
            s = s.split(":", 1)[0].strip()
        if not s:
            return None
        return f'{s}"'

    return f'{mi}"'



# ─────────────────────────────────────────
# GAME / SUBS FETCH
# ─────────────────────────────────────────
def fetch_candidate_games(now_utc: datetime) -> List[Dict[str, Any]]:
    """
    후보 게임: window(과거/미래) 내 + (리그 제한 있으면 적용)
    점수/상태는 hockey_games(=DB truth)만 사용.
    """
    time_min = now_utc.timestamp() - (PAST_DAYS * 86400)
    time_max = now_utc.timestamp() + (FUTURE_DAYS * 86400)

    where = ["EXTRACT(EPOCH FROM g.game_date) BETWEEN %s AND %s"]
    params: List[Any] = [time_min, time_max]

    if LEAGUE_SET:
        where.append("g.league_id = ANY(%s)")
        params.append(list(LEAGUE_SET))

    sql = f"""
        SELECT
          g.id,
          g.league_id,
          g.game_date,
          g.status,
          g.status_long,
          g.score_json,
          g.home_team_id,
          g.away_team_id,
          th.name AS home_name,
          ta.name AS away_name
        FROM hockey_games g
        LEFT JOIN hockey_teams th ON th.id = g.home_team_id
        LEFT JOIN hockey_teams ta ON ta.id = g.away_team_id
        WHERE {" AND ".join(where)}
        ORDER BY g.game_date ASC
        LIMIT %s
    """
    params.append(BATCH_LIMIT)
    return fetch_all(sql, tuple(params))


def fetch_subscriptions_for_games(game_ids: Sequence[int]) -> List[Subscription]:
    if not game_ids:
        return []
    rows = fetch_all(
        """
        SELECT
          s.device_id,
          d.fcm_token,
          s.game_id,
          s.notify_score,
          s.notify_game_start,
          s.notify_game_end,
          s.notify_periods
        FROM hockey_game_notification_subscriptions s
        JOIN hockey_user_devices d ON d.device_id = s.device_id
        WHERE s.game_id = ANY(%s)
        """,
        (list(game_ids),),
    )
    out: List[Subscription] = []
    for r in rows:
        out.append(
            Subscription(
                device_id=str(r.get("device_id") or ""),
                fcm_token=str(r.get("fcm_token") or ""),
                game_id=_to_int(r.get("game_id"), 0),
                notify_score=bool(r.get("notify_score", True)),
                notify_game_start=bool(r.get("notify_game_start", True)),
                notify_game_end=bool(r.get("notify_game_end", True)),
                notify_periods=bool(r.get("notify_periods", True)),
            )
        )
    return [x for x in out if x.device_id and x.fcm_token and x.game_id]

def fetch_subscription_rows(now_utc: datetime) -> List[Dict[str, Any]]:
    """
    ✅ 구독 우선:
    - 구독된 game_id를 먼저 확정하고,
    - hockey_games를 조인해서 (점수/상태/팀명 포함) 한 번에 가져온다.
    - game_date window로만 제한해서 "오래된 구독"은 매 tick마다 보지 않게 한다.
    - 알림 baseline용으로 subscription.updated_at 포함
    """
    time_min = now_utc.timestamp() - (PAST_DAYS * 86400)
    time_max = now_utc.timestamp() + (FUTURE_DAYS * 86400)

    sql = """
        SELECT
          s.device_id,
          d.fcm_token,
          s.game_id,
          s.notify_score,
          s.notify_game_start,
          s.notify_game_end,
          s.notify_periods,
          s.updated_at AS subscribed_at,

          g.league_id,
          g.game_date,
          g.status,
          g.status_long,
          g.score_json,
          g.raw_json,
          g.home_team_id,
          g.away_team_id,
          th.name AS home_name,
          ta.name AS away_name

        FROM hockey_game_notification_subscriptions s
        JOIN hockey_user_devices d ON d.device_id = s.device_id
        JOIN hockey_games g ON g.id = s.game_id
        LEFT JOIN hockey_teams th ON th.id = g.home_team_id
        LEFT JOIN hockey_teams ta ON ta.id = g.away_team_id
        WHERE EXTRACT(EPOCH FROM g.game_date) BETWEEN %s AND %s
    """
    return fetch_all(sql, (time_min, time_max))


# ─────────────────────────────────────────
# NOTIF MESSAGE
# ─────────────────────────────────────────
def build_hockey_message(
    event_type: str,
    g: Dict[str, Any],
    home: int,
    away: int,
    *,
    period: str = "",
    minute: Any = None,
    team_name: str = "",
    tag: str = "",
    status_norm: str = "",
) -> Tuple[str, str]:
    home_name = str(g.get("home_name") or "Home")
    away_name = str(g.get("away_name") or "Away")
    score_line = f"{home_name} {home} : {away} {away_name}"

    # ✅ period는 유지, minute은 무시
    prefix = period.strip() if period else ""

    if event_type == "goal":
        who = team_name or "Goal"
        title = f"🏒 {prefix} {who} Goal!" if prefix else f"🏒 {who} Goal!"
        body = score_line
        if tag:
            body = f"{score_line}\n{tag}"
        return (title, body)

    # ✅ 정정/취소 알림 추가 (점수 감소 기반)
    if event_type == "score_corrected":
        who = (team_name or "").strip()
        if who:
            title = f"🚫 {prefix} {who} Goal Cancelled" if prefix else f"🚫 {who} Goal Cancelled"
        else:
            title = f"🔄 {prefix} Score Corrected" if prefix else "🔄 Score Corrected"
        body = score_line
        if tag:
            body = f"{score_line}\n{tag}"
        return (title, body)

    if event_type == "game_start":
        return ("▶ Game Start", score_line)

    if event_type == "period_start":
        label = status_norm or "Period"
        return (f"▶ {label} Start", score_line)

    if event_type == "period_end":
        label = status_norm or "Period"
        return (f"⏸ {label} End", score_line)

    if event_type == "ot_start":
        return ("▶ Overtime", score_line)

    if event_type == "so_start":
        return ("🥅 Shootout", score_line)

    if event_type == "final":
        return ("⏱ Final", score_line)

    return ("Hockey Update", score_line)



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
# FSM TICK
# ─────────────────────────────────────────
def run_once() -> bool:
    """
    returns:
      - True  => fast interval recommended
      - False => slow interval recommended
    """
    now_utc = datetime.now(timezone.utc)

    # ─────────────────────────────
    # 1) 구독 우선 로드
    # ─────────────────────────────
    sub_rows = fetch_subscription_rows(now_utc)
    if not sub_rows:
        log.info("tick: subs=0 (window=%sd/%sd)", PAST_DAYS, FUTURE_DAYS)
        return False

    # fast 후보
    now_ts = now_utc.timestamp()
    has_fast_candidate = False
    if FAST_LEAGUE_SET:
        for r in sub_rows:
            try:
                lg = int(r.get("league_id") or 0)
            except Exception:
                continue
            if lg not in FAST_LEAGUE_SET:
                continue
            gd = r.get("game_date")
            if isinstance(gd, datetime):
                gd_ts = gd.timestamp()
                if (now_ts - 6 * 3600) <= gd_ts <= (now_ts + 6 * 3600):
                    has_fast_candidate = True
                    break

    # ─────────────────────────────
    # 2) Subscription / Game Map
    # ─────────────────────────────
    subs: List[Subscription] = []
    game_map: Dict[int, Dict[str, Any]] = {}

    for r in sub_rows:
        game_id = _to_int(r.get("game_id"), 0)
        device_id = str(r.get("device_id") or "").strip()
        token = str(r.get("fcm_token") or "").strip()
        if not (game_id and device_id and token):
            continue

        subs.append(
            Subscription(
                device_id=device_id,
                fcm_token=token,
                game_id=game_id,
                notify_score=bool(r.get("notify_score", True)),
                notify_game_start=bool(r.get("notify_game_start", True)),
                notify_game_end=bool(r.get("notify_game_end", True)),
                notify_periods=bool(r.get("notify_periods", True)),
            )
        )

        if game_id not in game_map:
            game_map[game_id] = {
                "id": game_id,
                "league_id": r.get("league_id"),
                "game_date": r.get("game_date"),
                "status": r.get("status"),
                "status_long": r.get("status_long"),
                "score_json": r.get("score_json"),
                "raw_json": r.get("raw_json"),
                "subscribed_at": r.get("subscribed_at"),
                "home_team_id": r.get("home_team_id"),
                "away_team_id": r.get("away_team_id"),
                "home_name": r.get("home_name"),
                "away_name": r.get("away_name"),
            }

    if not subs:
        return has_fast_candidate

    sent = 0

    # ─────────────────────────────
    # 3) FSM LOOP
    # ─────────────────────────────
    for sub in subs:
        g = game_map.get(sub.game_id)
        if not g:
            continue

        # ✅ 먼저 state 로드해서 last_* 확보
        st = load_state(sub.device_id, sub.game_id)
        state_exists = bool(st.get("_exists"))

        status_raw = str(g.get("status") or "").strip()
        status_norm = normalize_status(status_raw)

        # ✅ 알림은 canonical DB score_json만 사용
        db_home, db_away = extract_db_scores_only(g.get("score_json"))

        subscribed_at = g.get("subscribed_at")
        state_updated_at = st.get("updated_at")
        needs_reseed = (not state_exists) or _is_subscription_newer_than_state(
            subscribed_at,
            state_updated_at,
        )

        if needs_reseed:
            # ✅ 신규 구독 or 재구독/재설정:
            # 현재 DB 상태를 baseline으로 저장만 하고, 이 tick에서는 어떤 알림도 보내지 않음
            last_status = None
            last_status_norm = ""

            baseline_home = db_home if db_home is not None else 0
            baseline_away = db_away if db_away is not None else 0

            save_state(
                device_id=sub.device_id,
                game_id=sub.game_id,
                last_status=status_raw,
                last_home_score=baseline_home,
                last_away_score=baseline_away,
                sent_event_keys=["__score_epoch:0"],
            )
            continue

        last_status = st.get("last_status")
        last_status_norm = normalize_status(last_status)
        last_home = _to_int(st.get("last_home_score"), 0)
        last_away = _to_int(st.get("last_away_score"), 0)
        sent_keys: List[str] = list(st.get("sent_event_keys") or [])

        # - 둘 다 None → 점수 미확정 tick → last 값 유지
        # - 한쪽만 None → 그쪽만 last 유지
        if db_home is None and db_away is None:
            home, away = last_home, last_away
        else:
            home = db_home if db_home is not None else last_home
            away = db_away if db_away is not None else last_away

        # ─────────────────────────
        # score epoch (취소 후 재득점 재알림용)
        # - sent_event_keys에 "__score_epoch:N" 한 개를 저장
        # - 점수 감소(정정/취소)가 감지되면 epoch += 1
        # ─────────────────────────
        score_epoch = 0
        for k in sent_keys:
            if isinstance(k, str) and k.startswith("__score_epoch:"):
                try:
                    score_epoch = int(k.split(":", 1)[1])
                except Exception:
                    score_epoch = 0
        sent_keys = [k for k in sent_keys if not (isinstance(k, str) and k.startswith("__score_epoch:"))]
        sent_keys.append(f"__score_epoch:{score_epoch}")

        def _set_epoch(new_epoch: int) -> None:
            nonlocal score_epoch, sent_keys
            score_epoch = max(0, int(new_epoch))
            sent_keys = [k for k in sent_keys if not (isinstance(k, str) and k.startswith("__score_epoch:"))]
            sent_keys.append(f"__score_epoch:{score_epoch}")

        # ─────────────────────────
        # 공통: 이벤트 1회 발송 dedupe + 즉시 state 저장
        # ─────────────────────────
        def _send_once(event_key: str, title: str, body: str) -> None:
            nonlocal sent, sent_keys
            if event_key in sent_keys:
                return
            if send_push(sub.fcm_token, title, body, {"sport": "hockey", "game_id": str(sub.game_id)}):
                sent += 1
                sent_keys.append(event_key)

                # 🔒 즉시 state 저장 (플랩/중복 tick에서도 재발송 차단)
                save_state(
                    device_id=sub.device_id,
                    game_id=sub.game_id,
                    last_status=status_raw,
                    last_home_score=home,
                    last_away_score=away,
                    sent_event_keys=sent_keys,
                )
                time.sleep(SEND_SLEEP_SEC)

        # ─────────────────────────
        # (A) STATUS FSM (Period Start/End)
        # ─────────────────────────

        # game_start: NS -> 1P
        if sub.notify_game_start and status_norm == "1P" and last_status_norm != "1P":
            t, b = build_hockey_message("game_start", g, home, away)
            _send_once(f"gs:{sub.game_id}", t, b)

        # 1P end: 1P -> BT
        if sub.notify_periods and last_status_norm == "1P" and status_norm == "BT":
            t, b = build_hockey_message("period_end", g, home, away, status_norm="1P")
            _send_once(f"pe:{sub.game_id}:1P", t, b)

        # 2P start: BT -> 2P
        if sub.notify_periods and last_status_norm == "BT" and status_norm == "2P":
            t, b = build_hockey_message("period_start", g, home, away, status_norm="2P")
            _send_once(f"ps:{sub.game_id}:2P", t, b)

        # 2P end: 2P -> BT
        if sub.notify_periods and last_status_norm == "2P" and status_norm == "BT":
            t, b = build_hockey_message("period_end", g, home, away, status_norm="2P")
            _send_once(f"pe:{sub.game_id}:2P", t, b)

        # 3P start: BT -> 3P
        if sub.notify_periods and last_status_norm == "BT" and status_norm == "3P":
            t, b = build_hockey_message("period_start", g, home, away, status_norm="3P")
            _send_once(f"ps:{sub.game_id}:3P", t, b)

        # 3P 종료 + OT/SO/Final 점프 대응
        if sub.notify_periods and last_status_norm == "3P" and status_norm in ("OT", "SO", "FT", "AP", "AOT"):
            # ✅ 정규시간(3P) 종료 시점에 동점이 아니면 3P End는 스킵하고 Final만 가도록
            if not (status_norm == "FT" and home != away):
                t, b = build_hockey_message("period_end", g, home, away, status_norm="3P")
                _send_once(f"pe:{sub.game_id}:3P", t, b)

            if status_norm == "OT":
                t2, b2 = build_hockey_message("ot_start", g, home, away)
                _send_once(f"os:{sub.game_id}", t2, b2)
            elif status_norm == "SO":
                t2, b2 = build_hockey_message("so_start", g, home, away)
                _send_once(f"ss:{sub.game_id}", t2, b2)

        # OT -> SO start
        if sub.notify_periods and last_status_norm == "OT" and status_norm == "SO":
            t, b = build_hockey_message("so_start", g, home, away)
            _send_once(f"ss:{sub.game_id}", t, b)

        # ─────────────────────────
        # (B) SCORE / FINAL
        # ─────────────────────────
        score_changed = (home, away) != (last_home, last_away)
        score_increased = (home > last_home) or (away > last_away)
        score_decreased = (home < last_home) or (away < last_away)

        # ✅ 점수 알림은 실제 경기 진행 상태에서만 허용
        #    BT/NS/FT/AP/AOT 에서는 점수 변동을 state에만 반영하고 알림은 보내지 않음
        score_notify_live = is_score_notify_live_status(status_norm)

        became_final = is_final_status(status_norm) and not is_final_status(last_status_norm)
        decided_in_ot_or_so = last_status_norm in ("OT", "SO") and score_changed and is_final_status(status_norm)

        # ✅ 정정/취소(감소) 알림: 경기 진행 중일 때만
        if sub.notify_score and score_notify_live and score_decreased:
            _set_epoch(score_epoch + 1)

            team_name = ""
            if home < last_home:
                team_name = g.get("home_name") or "Home"
            elif away < last_away:
                team_name = g.get("away_name") or "Away"

            corr_key = f"corr:{sub.game_id}:e{score_epoch}:{last_home}-{last_away}->{home}-{away}"
            if corr_key not in sent_keys:
                t, b = build_hockey_message(
                    "score_corrected",
                    g,
                    home,
                    away,
                    team_name=team_name,
                    period=status_norm,
                )
                _send_once(corr_key, t, b)

        # ✅ 골 알림(증가): 경기 진행 중일 때만
        if sub.notify_score and score_notify_live and score_increased:
            team_name = ""
            if home > last_home:
                team_name = g.get("home_name") or "Home"
            elif away > last_away:
                team_name = g.get("away_name") or "Away"

            goal_key = f"goal:{sub.game_id}:e{score_epoch}:{last_home}-{last_away}->{home}-{away}"
            if goal_key not in sent_keys:
                t, b = build_hockey_message(
                    "goal",
                    g,
                    home,
                    away,
                    team_name=team_name,
                    period=status_norm,
                )
                _send_once(goal_key, t, b)

        # ✅ BT/종료상태에서 점수가 흔들려도 다음 재득점 알림 꼬이지 않게 epoch/state는 흡수
        if sub.notify_score and (not score_notify_live) and score_decreased:
            _set_epoch(score_epoch + 1)

        # Final 중복 방지
        if sub.notify_game_end and (became_final or decided_in_ot_or_so):
            final_key = f"final:{sub.game_id}"
            if final_key not in sent_keys:
                t, b = build_hockey_message("final", g, home, away)
                _send_once(final_key, t, b)

        # ─────────────────────────
        # (C) STATE SAVE (항상)
        # ─────────────────────────
        save_state(
            device_id=sub.device_id,
            game_id=sub.game_id,
            last_status=status_raw,
            last_home_score=home,
            last_away_score=away,
            sent_event_keys=sent_keys,
        )

    log.info("tick: sent=%d", sent)
    return has_fast_candidate





def run_forever(interval_sec: int) -> None:
    ensure_tables()

    log.info(
        "worker start(FSM): interval=%ss leagues=%s window=%sd/%sd batch=%d fast_leagues=%s fast=%ss slow=%ss",
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
            use_fast = run_once()
        except Exception as e:
            log.exception("tick failed: %s", e)

        if FAST_LEAGUE_SET and use_fast:
            sleep_sec = max(1, FAST_INTERVAL_SEC)
        else:
            sleep_sec = max(1, SLOW_INTERVAL_SEC)

        time.sleep(sleep_sec)


if __name__ == "__main__":
    run_forever(INTERVAL_SEC)

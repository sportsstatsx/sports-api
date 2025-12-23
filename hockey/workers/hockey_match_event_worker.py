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
          sent_event_keys
        FROM hockey_game_notification_states
        WHERE device_id=%s AND game_id=%s
        """,
        (device_id, game_id),
    )

    # ✅ state가 아직 없을 때 기본값 명확히
    return row or {
        "last_status": None,
        "last_home_score": 0,
        "last_away_score": 0,
        "sent_event_keys": [],
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

          g.league_id,
          g.game_date,
          g.status,
          g.status_long,
          g.score_json,
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

    if event_type == "goal":
        who = team_name or "Goal"

        parts: List[str] = []
        if period:
            parts.append(period)
        if minute is not None and str(minute).strip():
            parts.append(str(minute).strip())

        prefix = " ".join(parts)

        if prefix:
            title = f"🏒 {prefix} {who} Goal!"
        else:
            title = f"🏒 {who} Goal!"

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

        status_raw = str(g.get("status") or "").strip()
        status_norm = normalize_status(status_raw)
        home, away = parse_score(g.get("score_json"))

        st = load_state(sub.device_id, sub.game_id)
        last_status = st.get("last_status")
        last_status_norm = normalize_status(last_status)
        last_home = _to_int(st.get("last_home_score"), 0)
        last_away = _to_int(st.get("last_away_score"), 0)
        sent_keys: List[str] = list(st.get("sent_event_keys") or [])

        # ─────────────────────────
        # (A) STATUS FSM
        # ─────────────────────────
        if sub.notify_game_start and status_norm == "1P" and last_status_norm != "1P":
            t, b = build_hockey_message("game_start", g, home, away)
            if send_push(sub.fcm_token, t, b, {"sport": "hockey", "game_id": str(sub.game_id)}):
                sent += 1
                time.sleep(SEND_SLEEP_SEC)

        # 3P → OT/SO/Final 점프 대응
        if sub.notify_periods and last_status_norm == "3P" and status_norm in ("OT", "SO", "FT", "AP", "AOT"):
            t, b = build_hockey_message("period_end", g, home, away, status_norm="3P")
            if send_push(sub.fcm_token, t, b, {"sport": "hockey", "game_id": str(sub.game_id)}):
                sent += 1
                time.sleep(SEND_SLEEP_SEC)

            if status_norm == "OT":
                t2, b2 = build_hockey_message("ot_start", g, home, away)
                if send_push(sub.fcm_token, t2, b2, {"sport": "hockey", "game_id": str(sub.game_id)}):
                    sent += 1
                    time.sleep(SEND_SLEEP_SEC)

            elif status_norm == "SO":
                t2, b2 = build_hockey_message("so_start", g, home, away)
                if send_push(sub.fcm_token, t2, b2, {"sport": "hockey", "game_id": str(sub.game_id)}):
                    sent += 1
                    time.sleep(SEND_SLEEP_SEC)

        # ─────────────────────────
        # (B) SCORE / FINAL (옵션 A)
        # ─────────────────────────
        score_changed = (home, away) != (last_home, last_away)
        became_final = is_final_status(status_norm) and not is_final_status(last_status_norm)
        decided_in_ot_or_so = last_status_norm in ("OT", "SO") and score_changed

        # ── 골 중복 방지 (DB 기반)
        goal_key = f"goal:{sub.game_id}:{home}:{away}"
        if sub.notify_score and score_changed and goal_key not in sent_keys:
            team_name = ""
            if home > last_home:
                team_name = g.get("home_name") or "Home"
            elif away > last_away:
                team_name = g.get("away_name") or "Away"

            t, b = build_hockey_message(
                "goal",
                g,
                home,
                away,
                team_name=team_name,
                period=status_norm,
                minute=g.get("status_long"),
            )

            if send_push(sub.fcm_token, t, b, {"sport": "hockey", "game_id": str(sub.game_id)}):
                sent += 1
                sent_keys.append(goal_key)

                # 🔒 중요: 즉시 state 저장 (race 차단)
                save_state(
                    device_id=sub.device_id,
                    game_id=sub.game_id,
                    last_status=status_raw,
                    last_home_score=home,
                    last_away_score=away,
                    sent_event_keys=sent_keys,
                )
                time.sleep(SEND_SLEEP_SEC)

        # ── Final 중복 방지
        final_key = f"final:{sub.game_id}"
        if sub.notify_game_end and (became_final or decided_in_ot_or_so) and final_key not in sent_keys:
            t, b = build_hockey_message("final", g, home, away)
            if send_push(sub.fcm_token, t, b, {"sport": "hockey", "game_id": str(sub.game_id)}):
                sent += 1
                sent_keys.append(final_key)
                time.sleep(SEND_SLEEP_SEC)

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

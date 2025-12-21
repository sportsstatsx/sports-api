# hockey/workers/hockey_match_event_worker.py
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
# ENV (키는 기존 그대로 유지)
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

INTERVAL_SEC = _env_int("HOCKEY_MATCH_WORKER_INTERVAL_SEC", 10)

FAST_LEAGUES_RAW = _env_str("HOCKEY_MATCH_WORKER_FAST_LEAGUES", "")
FAST_INTERVAL_SEC = _env_int("HOCKEY_MATCH_WORKER_FAST_INTERVAL_SEC", 5)
SLOW_INTERVAL_SEC = _env_int("HOCKEY_MATCH_WORKER_SLOW_INTERVAL_SEC", INTERVAL_SEC)

LEAGUES_RAW = _env_str("HOCKEY_LIVE_LEAGUES", "")
PAST_DAYS = _env_int("HOCKEY_MATCH_WORKER_PAST_DAYS", 1)
FUTURE_DAYS = _env_int("HOCKEY_MATCH_WORKER_FUTURE_DAYS", 1)
BATCH_LIMIT = _env_int("HOCKEY_MATCH_WORKER_BATCH_LIMIT", 200)
MAX_EVENTS_PER_GAME_PER_TICK = _env_int("HOCKEY_MATCH_WORKER_MAX_EVENTS_PER_GAME_PER_TICK", 30)

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


FINAL_STATUSES = {
    "FT",
    "AOT",
    "AP",
    "AET",
    "PEN",
    "CANC",
    "PST",
    "ABD",
    "WO",
}

# ─────────────────────────────────────────
# DB
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


def ensure_tables() -> None:
    # 기존 테이블/컬럼은 네 DB에 이미 존재하지만, 안전하게 보강
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

    # ✅ sent_event_keys는 “남아 있어도 무시”한다 (호환 유지)
    execute(
        "ALTER TABLE hockey_game_notification_states "
        "ADD COLUMN IF NOT EXISTS sent_event_keys TEXT[] NOT NULL DEFAULT '{}'::text[];"
    )

    # ✅ 새 디듀프 테이블 (없으면 생성)
    execute(
        """
        CREATE TABLE IF NOT EXISTS hockey_notification_sent (
          device_id text NOT NULL,
          game_id integer NOT NULL,
          dedupe_key text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (device_id, game_id, dedupe_key),
          FOREIGN KEY (device_id) REFERENCES hockey_user_devices(device_id) ON DELETE CASCADE,
          FOREIGN KEY (game_id) REFERENCES hockey_games(id) ON DELETE CASCADE
        );
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_hockey_notification_sent_game ON hockey_notification_sent (game_id);")
    execute(
        "CREATE INDEX IF NOT EXISTS idx_hockey_notification_sent_created_at ON hockey_notification_sent (created_at);"
    )


# ─────────────────────────────────────────
# SCORE / STATUS PARSE (원본 로직 유지)
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

    if "home" in obj and "away" in obj and isinstance(obj.get("home"), (int, float, str)) and isinstance(
        obj.get("away"), (int, float, str)
    ):
        return _to_int(obj.get("home")), _to_int(obj.get("away"))

    for k in ("total", "totals", "final", "score"):
        v = obj.get(k)
        if isinstance(v, dict) and "home" in v and "away" in v:
            return _to_int(v.get("home")), _to_int(v.get("away"))

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


def normalize_status(status: Optional[str]) -> str:
    # ✅ 네 원본 표준화 유지 (P1/P2/P3 -> 1P/2P/3P)
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
# NOTIFICATION PAYLOAD (✅ 너 원본 그대로)
# ─────────────────────────────────────────
def build_matchup(game_row: Dict[str, Any]) -> str:
    home_name = str(game_row.get("home_name") or "Home")
    away_name = str(game_row.get("away_name") or "Away")
    return f"{home_name} vs {away_name}"


def build_score_line(game_row: Dict[str, Any], home: int, away: int) -> str:
    home_name = str(game_row.get("home_name") or "Home")
    away_name = str(game_row.get("away_name") or "Away")
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
        else:
            tag_line = ""

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
# CORE
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
        SELECT device_id, game_id, last_status, last_home_score, last_away_score, last_event_id
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
    start = now_utc.timestamp() - (PAST_DAYS * 86400)
    end = now_utc.timestamp() + (FUTURE_DAYS * 86400)

    league_clause = ""
    params: List[Any] = []
    params.extend([datetime.fromtimestamp(start, tz=timezone.utc), datetime.fromtimestamp(end, tz=timezone.utc)])

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


def fetch_new_events(game_id: int, last_event_id: int) -> List[Dict[str, Any]]:
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
            event_order,
            event_key,
            notif_key,
            updated_at
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


def _hash_key(s: str) -> str:
    raw = (s or "").encode("utf-8", errors="ignore")
    return "h1:" + hashlib.sha1(raw).hexdigest()


def claim_send_once(device_id: str, game_id: int, dedupe_key: str) -> bool:
    """
    ✅ 핵심: (device_id, game_id, dedupe_key) PK로 DB 원자적 디듀프
    """
    row = fetch_one(
        """
        INSERT INTO hockey_notification_sent (device_id, game_id, dedupe_key)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING 1
        """,
        (device_id, game_id, dedupe_key),
    )
    return row is not None


def run_once() -> bool:
    now_utc = datetime.now(timezone.utc)
    games = fetch_candidate_games(now_utc)

    if not games:
        log.info("tick: candidates=0")
        return False

    # fast 후보 판별(원본 유지)
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
        last_status_norm = normalize_status(last_status)
        status_norm = normalize_status(status)

        # ─────────────────────────────
        # (A) 상태 전환 알림 (✅ 문구/데이터는 원본 build_hockey_message/send_push 그대로)
        # 디듀프는 hockey_notification_sent 로 처리
        # ─────────────────────────────
        def _send_status_once(ntype: str, title: str, body: str) -> None:
            nonlocal sent
            dk = _hash_key(f"{sub.game_id}:status:{ntype}")
            if not claim_send_once(sub.device_id, sub.game_id, dk):
                return
            ok = send_push(
                token=sub.fcm_token,
                title=title,
                body=body,
                data={"sport": "hockey", "game_id": str(sub.game_id), "type": ntype, "status": status},
            )
            if ok:
                sent += 1
                time.sleep(SEND_SLEEP_SEC)

        if sub.notify_game_start and (status_norm == "1P") and (last_status_norm != "1P"):
            t, b = build_hockey_message("game_start", g, home, away)
            _send_status_once("game_start", t, b)

        if sub.notify_periods and (last_status_norm == "1P") and (status_norm == "BT"):
            t, b = build_hockey_message("period_end", g, home, away, status_norm="1P")
            _send_status_once("period_end_1", t, b)

        if sub.notify_periods and (last_status_norm == "BT") and (status_norm == "2P"):
            t, b = build_hockey_message("period_start", g, home, away, status_norm="2P")
            _send_status_once("period_start_2", t, b)

        if sub.notify_periods and (last_status_norm == "2P") and (status_norm == "BT"):
            t, b = build_hockey_message("period_end", g, home, away, status_norm="2P")
            _send_status_once("period_end_2", t, b)

        if sub.notify_periods and (last_status_norm == "BT") and (status_norm == "3P"):
            t, b = build_hockey_message("period_start", g, home, away, status_norm="3P")
            _send_status_once("period_start_3", t, b)

        if sub.notify_periods and (last_status_norm == "3P") and (status_norm == "OT"):
            t, b = build_hockey_message("ot_start", g, home, away)
            _send_status_once("ot_start", t, b)

        if sub.notify_periods and (last_status_norm == "OT") and (status_norm == "SO"):
            t, b = build_hockey_message("so_start", g, home, away)
            _send_status_once("so_start", t, b)

        if sub.notify_game_end and is_final_status(status_norm) and (not is_final_status(last_status_norm)):
            t, b = build_hockey_message("final", g, home, away)
            _send_status_once("final", t, b)

        # ─────────────────────────────
        # (B) 이벤트 알림: GOAL
        # ✅ 디듀프 키는 event_key로 고정 (DB에 항상 존재 + 유니크)
        # ✅ 문구는 build_hockey_message 그대로
        # ─────────────────────────────
        new_events = fetch_new_events(sub.game_id, last_event_id)
        if len(new_events) > MAX_EVENTS_PER_GAME_PER_TICK:
            new_events = new_events[-MAX_EVENTS_PER_GAME_PER_TICK :]

        max_seen_event_id = last_event_id

        for ev in new_events:
            ev_id = _to_int(ev.get("id"), 0)
            if ev_id > max_seen_event_id:
                max_seen_event_id = ev_id

            etype = str(ev.get("type") or "").strip().lower()
            if etype != "goal":
                continue
            if not sub.notify_score:
                continue

            # ✅ 핵심: event_key 사용
            ek = str(ev.get("event_key") or "").strip()
            if not ek:
                # 이론상 없어야 함 (너 DB에서 event_key_empty=0이었음)
                # 그래도 안전장치
                ek = f"fallback|{ev.get('period')}|{ev.get('minute')}|{ev.get('team_id')}|{ev_id}"

            dk = _hash_key(f"{sub.game_id}:event:{ek}")
            if not claim_send_once(sub.device_id, sub.game_id, dk):
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

            # ✅ 점수는 항상 score_json 기준 (원본 정책 유지)
            notif_home = home
            notif_away = away

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
                data={"sport": "hockey", "game_id": str(sub.game_id), "type": etype, "status": status},
            )
            if ok:
                sent += 1
                time.sleep(SEND_SLEEP_SEC)

        # state 저장 (sent_event_keys는 건드리지 않음/호환용으로 그냥 둠)
        save_state(
            device_id=sub.device_id,
            game_id=sub.game_id,
            last_status=status,
            last_home_score=home,
            last_away_score=away,
            last_event_id=max_seen_event_id,
        )

    log.info("tick: sent=%d", sent)
    return has_fast_candidate


def run_forever(interval_sec: int) -> None:
    ensure_tables()
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

# hockey/workers/hockey_match_event_worker.py
from __future__ import annotations

import hashlib
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
# ENV (기존 키 그대로 유지)
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

LOG_SAMPLE_RATE = _env_float("LOG_SAMPLE_RATE", 0.25)  # 있으면 활용, 없으면 기본


def _parse_int_list(raw: str) -> List[int]:
    out: List[int] = []
    for p in (raw or "").split(","):
        s = p.strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except Exception:
            continue
    return out


LEAGUE_IDS = _parse_int_list(LEAGUES_RAW)
FAST_LEAGUE_IDS = _parse_int_list(FAST_LEAGUES_RAW)
FAST_LEAGUE_SET = set(FAST_LEAGUE_IDS)

# DB 상태상 status는 NS/P1/P2/BT/FT/AOT/AP/CANC/POST 등으로 옴
FINAL_STATUSES = {"FT", "AOT", "AP", "CANC", "POST"}  # 필요 시 확장


# ─────────────────────────────────────────
# DB Pool
# ─────────────────────────────────────────
pool = ConnectionPool(conninfo=HOCKEY_DATABASE_URL, max_size=5, kwargs={"autocommit": True})


def _hash_key(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _status_norm(s: Optional[str]) -> str:
    return (s or "").strip().upper()


def _score_from_json(score_json: Any) -> Tuple[int, int]:
    """
    hockey_games.score_json 구조는 네 서버 포맷 그대로 쓴다.
    여기서는 안전하게 여러 케이스를 허용하고, 없으면 0,0.
    """
    try:
        if isinstance(score_json, str):
            score_json = json.loads(score_json)
    except Exception:
        score_json = {}

    if not isinstance(score_json, dict):
        return 0, 0

    # 1) {"home": 1, "away": 2}
    if isinstance(score_json.get("home"), (int, float)) and isinstance(score_json.get("away"), (int, float)):
        return int(score_json.get("home") or 0), int(score_json.get("away") or 0)

    # 2) {"total": {"home": 1, "away": 2}}
    total = score_json.get("total")
    if isinstance(total, dict):
        h = total.get("home")
        a = total.get("away")
        if isinstance(h, (int, float)) and isinstance(a, (int, float)):
            return int(h or 0), int(a or 0)

    # 3) {"scores": {"home": {"total": 1}, "away": {"total": 2}}}
    scores = score_json.get("scores")
    if isinstance(scores, dict):
        home = scores.get("home") if isinstance(scores.get("home"), dict) else {}
        away = scores.get("away") if isinstance(scores.get("away"), dict) else {}
        h = home.get("total")
        a = away.get("total")
        if isinstance(h, (int, float)) and isinstance(a, (int, float)):
            return int(h or 0), int(a or 0)

    return 0, 0


# ─────────────────────────────────────────
# MIGRATIONS (sent table)
# ─────────────────────────────────────────
def ensure_tables() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS hockey_notification_sent (
      device_id text NOT NULL,
      game_id integer NOT NULL,
      dedupe_key text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (device_id, game_id, dedupe_key),
      FOREIGN KEY (device_id) REFERENCES hockey_user_devices(device_id) ON DELETE CASCADE,
      FOREIGN KEY (game_id) REFERENCES hockey_games(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_hockey_notification_sent_game
      ON hockey_notification_sent (game_id);
    CREATE INDEX IF NOT EXISTS idx_hockey_notification_sent_created_at
      ON hockey_notification_sent (created_at);
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


# ─────────────────────────────────────────
# DB QUERIES (game 단위로 한번에)
# ─────────────────────────────────────────
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

    final_placeholders = ",".join(["%s"] * len(FINAL_STATUSES))

    sql = f"""
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
      at.name AS away_name,
      g.updated_at
    FROM hockey_games g
    LEFT JOIN hockey_teams ht ON ht.id = g.home_team_id
    LEFT JOIN hockey_teams at ON at.id = g.away_team_id
    WHERE g.game_date IS NOT NULL
      AND g.game_date >= %s
      AND g.game_date <= %s
      {league_clause}
      AND (
        COALESCE(UPPER(g.status), '') NOT IN ({final_placeholders})
        OR g.updated_at >= NOW() - interval '6 hours'
      )
    ORDER BY g.game_date DESC
    LIMIT {BATCH_LIMIT}
    """

    with pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, tuple(params + list(FINAL_STATUSES)))
            return list(cur.fetchall())


@dataclass
class SubRow:
    device_id: str
    game_id: int
    notify_score: bool
    notify_game_start: bool
    notify_game_end: bool
    notify_periods: bool
    fcm_token: str
    notifications_enabled: bool


def fetch_subscriptions_for_game(game_id: int) -> List[SubRow]:
    # notifications_enabled=false면 발송 X
    sql = """
    SELECT
      s.device_id,
      s.game_id,
      s.notify_score,
      s.notify_game_start,
      s.notify_game_end,
      s.notify_periods,
      d.fcm_token,
      d.notifications_enabled
    FROM hockey_game_notification_subscriptions s
    JOIN hockey_user_devices d ON d.device_id = s.device_id
    WHERE s.game_id = %s
      AND d.notifications_enabled = true
      AND d.fcm_token IS NOT NULL
      AND btrim(d.fcm_token) <> ''
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, (game_id,))
            rows = cur.fetchall()

    out: List[SubRow] = []
    for r in rows:
        out.append(
            SubRow(
                device_id=str(r["device_id"]),
                game_id=int(r["game_id"]),
                notify_score=bool(r["notify_score"]),
                notify_game_start=bool(r["notify_game_start"]),
                notify_game_end=bool(r["notify_game_end"]),
                notify_periods=bool(r["notify_periods"]),
                fcm_token=str(r["fcm_token"]),
                notifications_enabled=bool(r["notifications_enabled"]),
            )
        )
    return out


@dataclass
class StateRow:
    device_id: str
    game_id: int
    last_status: str
    last_home_score: int
    last_away_score: int
    last_event_id: int


def fetch_states_for_game(game_id: int, device_ids: Sequence[str]) -> Dict[str, StateRow]:
    if not device_ids:
        return {}

    sql = """
    SELECT device_id, game_id, last_status, last_home_score, last_away_score, last_event_id
    FROM hockey_game_notification_states
    WHERE game_id = %s
      AND device_id = ANY(%s)
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, (game_id, list(device_ids)))
            rows = cur.fetchall()

    out: Dict[str, StateRow] = {}
    for r in rows:
        out[str(r["device_id"])] = StateRow(
            device_id=str(r["device_id"]),
            game_id=int(r["game_id"]),
            last_status=str(r.get("last_status") or ""),
            last_home_score=int(r.get("last_home_score") or 0),
            last_away_score=int(r.get("last_away_score") or 0),
            last_event_id=int(r.get("last_event_id") or 0),
        )
    return out


def upsert_state(row: StateRow) -> None:
    sql = """
    INSERT INTO hockey_game_notification_states
      (device_id, game_id, last_status, last_home_score, last_away_score, last_event_id)
    VALUES
      (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (device_id, game_id)
    DO UPDATE SET
      last_status = EXCLUDED.last_status,
      last_home_score = EXCLUDED.last_home_score,
      last_away_score = EXCLUDED.last_away_score,
      last_event_id = EXCLUDED.last_event_id,
      updated_at = now()
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    row.device_id,
                    row.game_id,
                    row.last_status,
                    row.last_home_score,
                    row.last_away_score,
                    row.last_event_id,
                ),
            )


def fetch_events_for_game(game_id: int, min_last_event_id: int) -> List[Dict[str, Any]]:
    # ✅ id 증분 + 최근 updated_at 윈도우(UPDATE/리컨실 방지)
    sql = """
    SELECT
      id, period, minute, team_id, type, comment,
      players, assists, event_order, event_key, notif_key, updated_at
    FROM hockey_game_events
    WHERE game_id = %s
      AND (
        id > %s
        OR updated_at >= NOW() - interval '180 seconds'
      )
    ORDER BY id ASC
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, (game_id, min_last_event_id))
            return list(cur.fetchall())


# ─────────────────────────────────────────
# DEDUPE (DB row 기반)
# ─────────────────────────────────────────
def claim_send_once(device_id: str, game_id: int, dedupe_key: str) -> bool:
    """
    최초 1회만 True.
    """
    sql = """
    INSERT INTO hockey_notification_sent (device_id, game_id, dedupe_key)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING
    RETURNING 1
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (device_id, game_id, dedupe_key))
            got = cur.fetchone()
            return got is not None


def cleanup_sent_table(days: int = 14) -> None:
    sql = "DELETE FROM hockey_notification_sent WHERE created_at < NOW() - (%s || ' days')::interval"
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(days),))


# ─────────────────────────────────────────
# NOTIFICATION MESSAGE (기존 호출 문구/형식은 여기서 유지)
# ─────────────────────────────────────────
def build_title_body(kind: str, g: Dict[str, Any], home_score: int, away_score: int, extra: str = "") -> Tuple[str, str]:
    home_name = str(g.get("home_name") or "Home")
    away_name = str(g.get("away_name") or "Away")
    base = f"{home_name} {home_score}-{away_score} {away_name}"
    if kind == "goal":
        title = "GOAL"
        body = base if not extra else f"{base} · {extra}"
        return title, body
    if kind == "game_start":
        return "GAME START", base
    if kind == "period_start":
        return "PERIOD START", base if not extra else f"{base} · {extra}"
    if kind == "period_end":
        return "PERIOD END", base if not extra else f"{base} · {extra}"
    if kind == "final":
        return "FINAL", base
    return "UPDATE", base if not extra else f"{base} · {extra}"


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _send_fcm(client: FCMClient, token: str, title: str, body: str, data: Dict[str, str]) -> None:
    client.send_to_token(token=token, title=title, body=body, data=data)


# ─────────────────────────────────────────
# STATUS TRANSITIONS (P1/P2/P3/BT/OT/SO/FT/AOT/AP)
# ─────────────────────────────────────────
def is_final(status: str) -> bool:
    return _status_norm(status) in FINAL_STATUSES


def run_once() -> bool:
    now = _now_utc()

    # 가끔 청소(너무 자주 하면 부담이라 샘플링)
    if LOG_SAMPLE_RATE > 0 and (hash(int(now.timestamp())) % 100) < int(LOG_SAMPLE_RATE * 100):
        try:
            cleanup_sent_table(days=14)
        except Exception:
            log.debug("cleanup failed", exc_info=True)

    games = fetch_candidate_games(now)

    has_fast_candidate = False
    sent = 0

    fcm = FCMClient()  # 기존 구현 그대로 사용

    for g in games:
        game_id = int(g["id"])
        league_id = int(g.get("league_id") or 0)
        if league_id and league_id in FAST_LEAGUE_SET:
            has_fast_candidate = True

        subs = fetch_subscriptions_for_game(game_id)
        if not subs:
            continue

        device_ids = [s.device_id for s in subs]
        states = fetch_states_for_game(game_id, device_ids)

        # subs 중 최소 last_event_id를 기준으로 이벤트를 한 번만 읽는다
        min_last_event_id = min([states.get(s.device_id).last_event_id if states.get(s.device_id) else 0 for s in subs] or [0])
        events = fetch_events_for_game(game_id, min_last_event_id)

        # 스팸 방지: 너무 많으면 뒤에서 N개만
        if len(events) > MAX_EVENTS_PER_GAME_PER_TICK:
            events = events[-MAX_EVENTS_PER_GAME_PER_TICK :]

        status_now = _status_norm(g.get("status"))
        home_score_now, away_score_now = _score_from_json(g.get("score_json"))

        for sub in subs:
            st = states.get(sub.device_id) or StateRow(
                device_id=sub.device_id,
                game_id=game_id,
                last_status="",
                last_home_score=0,
                last_away_score=0,
                last_event_id=0,
            )

            last_status = _status_norm(st.last_status)

            # ─────────────────────────────
            # (A) 상태 알림 (전이 기반, DB dedupe)
            # ─────────────────────────────

            # GAME START: NS -> P1 (또는 NS -> P2 같은 비정상 시작도 방어)
            if sub.notify_game_start and (last_status == "NS") and (status_now in ("P1", "P2", "P3", "BT", "OT", "SO")):
                dk = _hash_key(f"{game_id}:status:game_start")
                if claim_send_once(sub.device_id, game_id, dk):
                    t, b = build_title_body("game_start", g, home_score_now, away_score_now)
                    _send_fcm(
                        fcm,
                        sub.fcm_token,
                        t,
                        b,
                        {"sport": "hockey", "game_id": str(game_id), "kind": "game_start"},
                    )
                    sent += 1
                    if SEND_SLEEP_SEC > 0:
                        time.sleep(SEND_SLEEP_SEC)

            # PERIOD START/END: status_long 분포상 BT가 break time으로 들어옴
            # - P1 -> BT : 1피리어드 종료
            # - BT -> P2 : 2피리어드 시작
            # - P2 -> BT : 2피리어드 종료
            # - BT -> P3 : 3피리어드 시작
            if sub.notify_periods:
                transitions = [
                    ("P1", "BT", "period_end", "P1"),
                    ("BT", "P2", "period_start", "P2"),
                    ("P2", "BT", "period_end", "P2"),
                    ("BT", "P3", "period_start", "P3"),
                ]
                for a, b, kind, label in transitions:
                    if last_status == a and status_now == b:
                        dk = _hash_key(f"{game_id}:status:{kind}:{label}")
                        if claim_send_once(sub.device_id, game_id, dk):
                            t, body = build_title_body(kind, g, home_score_now, away_score_now, extra=label)
                            _send_fcm(
                                fcm,
                                sub.fcm_token,
                                t,
                                body,
                                {"sport": "hockey", "game_id": str(game_id), "kind": kind, "period": label},
                            )
                            sent += 1
                            if SEND_SLEEP_SEC > 0:
                                time.sleep(SEND_SLEEP_SEC)

                # OT/SO는 리그마다 다름. DB status 분포에 AOT/AP가 “완료 상태”로 존재.
                # 여기서는 "진행상태 -> OT/SO" 전이만 잡고, 최종은 final에서 처리.
                if last_status in ("P3", "BT") and status_now == "OT":
                    dk = _hash_key(f"{game_id}:status:ot_start")
                    if claim_send_once(sub.device_id, game_id, dk):
                        t, body = build_title_body("period_start", g, home_score_now, away_score_now, extra="OT")
                        _send_fcm(
                            fcm,
                            sub.fcm_token,
                            t,
                            body,
                            {"sport": "hockey", "game_id": str(game_id), "kind": "ot_start"},
                        )
                        sent += 1
                        if SEND_SLEEP_SEC > 0:
                            time.sleep(SEND_SLEEP_SEC)

                if last_status == "OT" and status_now == "SO":
                    dk = _hash_key(f"{game_id}:status:so_start")
                    if claim_send_once(sub.device_id, game_id, dk):
                        t, body = build_title_body("period_start", g, home_score_now, away_score_now, extra="SO")
                        _send_fcm(
                            fcm,
                            sub.fcm_token,
                            t,
                            body,
                            {"sport": "hockey", "game_id": str(game_id), "kind": "so_start"},
                        )
                        sent += 1
                        if SEND_SLEEP_SEC > 0:
                            time.sleep(SEND_SLEEP_SEC)

            # FINAL
            if sub.notify_game_end and is_final(status_now) and (not is_final(last_status)):
                dk = _hash_key(f"{game_id}:status:final")
                if claim_send_once(sub.device_id, game_id, dk):
                    t, b = build_title_body("final", g, home_score_now, away_score_now)
                    _send_fcm(
                        fcm,
                        sub.fcm_token,
                        t,
                        b,
                        {"sport": "hockey", "game_id": str(game_id), "kind": "final"},
                    )
                    sent += 1
                    if SEND_SLEEP_SEC > 0:
                        time.sleep(SEND_SLEEP_SEC)

            # ─────────────────────────────
            # (B) 이벤트 알림: goal만 (기존 정책 유지)
            # ─────────────────────────────
            max_seen_event_id = st.last_event_id

            for ev in events:
                ev_id = _safe_int(ev.get("id"), 0)
                if ev_id > max_seen_event_id:
                    max_seen_event_id = ev_id

                # 각 device별로는 "내 last_event_id 이후"만 기본 처리
                # 단, updated_at 윈도우로 들어온 이벤트도 있을 수 있어 dedupe로 최종 방어
                if ev_id <= st.last_event_id and (ev.get("updated_at") is None):
                    continue

                etype = str(ev.get("type") or "").strip().lower()
                if etype != "goal":
                    continue

                if not sub.notify_score:
                    continue

                # ✅ dedupe는 event_key 기반으로 고정 (DB에서 항상 존재, game_id+event_key 유니크)
                ek = str(ev.get("event_key") or "").strip()
                if not ek:
                    # 이론상 없어야 함. 그래도 안전하게 fallback.
                    ek = f"fallback|{ev.get('period')}|{ev.get('minute')}|{ev.get('team_id')}|{ev.get('comment')}|{ev_id}"

                dk = _hash_key(f"{game_id}:event:{ek}")
                if not claim_send_once(sub.device_id, game_id, dk):
                    continue

                period = str(ev.get("period") or "").strip()
                minute = ev.get("minute")
                team_id = _safe_int(ev.get("team_id"), 0)

                # 표시용 extra (너의 기존 문구가 따로 있으면 여기서 교체하면 됨)
                extra = ""
                if period:
                    extra = f"{period}"
                if minute is not None:
                    extra = f"{extra} {minute}'".strip()

                # 점수는 항상 hockey_games.score_json 기준(보정 없음)
                t, b = build_title_body("goal", g, home_score_now, away_score_now, extra=extra)
                _send_fcm(
                    fcm,
                    sub.fcm_token,
                    t,
                    b,
                    {
                        "sport": "hockey",
                        "game_id": str(game_id),
                        "kind": "goal",
                        "period": period,
                        "minute": str(minute) if minute is not None else "",
                        "team_id": str(team_id) if team_id else "",
                        "event_id": str(ev_id),
                    },
                )
                sent += 1
                if SEND_SLEEP_SEC > 0:
                    time.sleep(SEND_SLEEP_SEC)

            # state 갱신 (sent_event_keys는 더 이상 사용 안 함)
            st.last_status = status_now
            st.last_home_score = home_score_now
            st.last_away_score = away_score_now
            st.last_event_id = max_seen_event_id
            upsert_state(st)

    log.info("tick: sent=%d games=%d", sent, len(games))
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

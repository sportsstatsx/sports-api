from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from basketball.nba.nba_db import nba_execute, nba_fetch_all, nba_fetch_one
from notifications.fcm_client import FCMClient

log = logging.getLogger("nba_match_event_worker")
logging.basicConfig(level=logging.INFO)

# ─────────────────────────────────────────
# ENV (하키 스타일)
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


DATABASE_URL = _env_str("NBA_DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError("NBA_DATABASE_URL is not set")

INTERVAL_SEC = _env_int("NBA_NOTIF_INTERVAL_SEC", 10)

# 후보 경기 window
LOOKBACK_MIN = _env_int("NBA_NOTIF_LOOKBACK_MIN", 240)   # 기본 4시간
LOOKAHEAD_MIN = _env_int("NBA_NOTIF_LOOKAHEAD_MIN", 60)  # 기본 1시간

# send sleep
SEND_SLEEP_SEC = _env_float("NBA_NOTIF_SEND_SLEEP_SEC", 0.08)

# fast/slow
FAST_INTERVAL_SEC = _env_int("NBA_NOTIF_FAST_INTERVAL_SEC", 2)
SLOW_INTERVAL_SEC = _env_int("NBA_NOTIF_SLOW_INTERVAL_SEC", 10)


# ─────────────────────────────────────────
# TABLES (이미 존재하지만 보강)
# ─────────────────────────────────────────
def ensure_tables() -> None:
    nba_execute(
        """
        CREATE TABLE IF NOT EXISTS nba_game_notification_states (
          device_id TEXT NOT NULL,
          game_id INTEGER NOT NULL,
          last_status TEXT,
          last_home_score INTEGER,
          last_away_score INTEGER,
          last_event_id BIGINT,
          created_at TIMESTAMPTZ DEFAULT now(),
          updated_at TIMESTAMPTZ DEFAULT now(),
          sent_event_keys TEXT[] DEFAULT '{}'::text[],
          PRIMARY KEY (device_id, game_id)
        );
        """,
        (),
    )
    nba_execute(
        "ALTER TABLE nba_game_notification_states "
        "ADD COLUMN IF NOT EXISTS sent_event_keys TEXT[] DEFAULT '{}'::text[];",
        (),
    )


# ─────────────────────────────────────────
# JSON / parse helpers (너 fixtures 규칙 최대한 유지)
# ─────────────────────────────────────────
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


def _json_obj(x: Any) -> dict:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            j = json.loads(x)
            return j if isinstance(j, dict) else {}
        except Exception:
            return {}
    return {}


def _extract_scores_from_raw(raw: dict) -> Tuple[Optional[int], Optional[int]]:
    cand_home = [
        (raw.get("scores") or {}).get("home", {}).get("points"),
        (raw.get("scores") or {}).get("home", {}).get("total"),
        (raw.get("score") or {}).get("home", {}).get("total"),
        (raw.get("score") or {}).get("home"),
    ]
    cand_away = [
        (raw.get("scores") or {}).get("visitors", {}).get("points"),
        (raw.get("scores") or {}).get("visitors", {}).get("total"),
        (raw.get("score") or {}).get("away", {}).get("total"),
        (raw.get("score") or {}).get("away"),
        (raw.get("score") or {}).get("visitors", {}).get("total"),
    ]
    home = next((v for v in (_safe_int(x) for x in cand_home) if v is not None), None)
    away = next((v for v in (_safe_int(x) for x in cand_away) if v is not None), None)
    return home, away


def _clock_text(raw: dict) -> Optional[str]:
    try:
        c = (((raw or {}).get("status") or {}).get("clock") or "")
        c = str(c).strip()
        return c or None
    except Exception:
        return None


def _halftime_flag(raw: dict) -> bool:
    try:
        return bool((((raw or {}).get("status") or {}).get("halftime") is True))
    except Exception:
        return False


def _count_filled_linescore(team_scores: dict) -> int:
    ls = (team_scores or {}).get("linescore") or []
    if not isinstance(ls, list):
        return 0
    return sum(1 for x in ls if str(x).strip() != "")


def _completed_units(raw: dict) -> int:
    """
    linescore에서 '이미 완료된 구간 수'를 계산.
    - Q1~Q4 = 1~4
    - OT1이 확정(라인스코어 5번째가 채워짐)되면 5
    - OT2 확정되면 6 ...
    """
    scores = raw.get("scores") or {}
    if not isinstance(scores, dict):
        return 0
    home_units = _count_filled_linescore(scores.get("home") or {})
    away_units = _count_filled_linescore(scores.get("visitors") or {})
    return max(home_units, away_units)


def _is_inplay(status_short: Any, status_long: str) -> bool:
    # DB 분포 확정: 2 = In Play
    try:
        return int(status_short) == 2
    except Exception:
        return str(status_long or "").strip().lower() == "in play"


def _is_final(status_short: Any, status_long: str) -> bool:
    # DB 분포 확정: 3 = Finished (Final)
    try:
        return int(status_short) == 3
    except Exception:
        return str(status_long or "").strip().lower() == "finished"


# ─────────────────────────────────────────
# PHASE 판정 (쿼터/OT 시작/종료만)
# ─────────────────────────────────────────
@dataclass
class Phase:
    """
    kind:
      - Q_START / Q_END
      - OT_START / OT_END
      - FINAL
      - GAME_START (옵션)
    index:
      - Q: 1..4
      - OT: 1..N
    """
    kind: str
    index: int = 0
    label: str = ""  # 알림 타이틀용(예: "1Q Start", "OT1 End")


def _detect_phase(
    *,
    status_short: Any,
    status_long: str,
    raw: dict,
    sent_keys: List[str],
) -> Optional[Phase]:
    """
    원칙:
    - score 알림은 절대 안 함.
    - clock 유무 + completed_units(완료된 구간 수) + (Q4_END sent 여부)로 OT를 추정.
    - 단계당 1회만 보내도록 event_key는 kind+index로 고정.
    """
    if _is_final(status_short, status_long):
    return Phase("FINAL", 0, "Final")

    if not _is_inplay(status_short, status_long):
        return None

    clock = _clock_text(raw)
    completed = _completed_units(raw)  # 0..(4+OT)

    # 공통: "게임 시작"은 InPlay 첫 진입에서 한 번
    # (네가 원하면 켜고, 아니면 그냥 주석 처리 가능)
    if "gs" not in sent_keys:
        # 첫 InPlay 감지 시점에만
        return Phase("GAME_START", 0, "Game Start")

    # clock이 있으면 "진행 시작" 상태
    if clock:
        # 1) 아직 Q1~Q4 범위일 가능성이 높음 (completed<=3이면 확정)
        if completed <= 3:
            q = completed + 1  # 0->Q1, 1->Q2, 2->Q3, 3->Q4
            return Phase("Q_START", q, f"{q}Q Start")

        # 2) completed>=4인 상태에서 clock이 다시 생기면 OT로 봐야 함
        #    OT1 시작은 "Q4 End를 이미 보냈다"를 기준으로 잡는다.
        if "qe:4" in sent_keys:
            # OT index는 "현재까지 완료된 구간 수 - 3"
            # completed=4 (OT1 진행중) => 1
            # completed=5 (OT2 진행중) => 2
            ot = max(1, completed - 3)
            return Phase("OT_START", ot, f"OT{ot} Start")

        # Q4가 아직 끝났다고 확정 못했으면 Q4 진행중 취급
        return Phase("Q_START", 4, "4Q Start")

    # clock이 없으면 "구간 종료/브레이크" 상태로 간주
    # halftime 플래그는 결국 Q2 종료 케이스라서 그냥 completed 기반 종료를 만든다.
    if 1 <= completed <= 4:
        # completed가 1이면 Q1 끝남, 2면 Q2 끝남 ...
        return Phase("Q_END", completed, f"{completed}Q End")

    if completed >= 5:
        # completed=5 => OT1 끝남, completed=6 => OT2 끝남 ...
        ot = completed - 4
        return Phase("OT_END", ot, f"OT{ot} End")

    return None


# ─────────────────────────────────────────
# DB fetch (구독 우선 조인)
# ─────────────────────────────────────────
def fetch_subscription_rows(now_utc: datetime) -> List[Dict[str, Any]]:
    start = now_utc - timedelta(minutes=LOOKBACK_MIN)
    end = now_utc + timedelta(minutes=LOOKAHEAD_MIN)

    return nba_fetch_all(
        """
        SELECT
          s.device_id,
          d.fcm_token,
          s.game_id,
          s.notify_game_start,
          s.notify_game_end,
          s.notify_periods,

          g.status_short,
          g.status_long,
          g.raw_json,

          th.name AS home_name,
          tv.name AS away_name

        FROM nba_game_notification_subscriptions s
        JOIN nba_user_devices d ON d.device_id = s.device_id
        JOIN nba_games g ON g.id = s.game_id
        LEFT JOIN nba_teams th ON th.id = g.home_team_id
        LEFT JOIN nba_teams tv ON tv.id = g.visitor_team_id
        WHERE g.date_start_utc >= %s
          AND g.date_start_utc <= %s
          AND COALESCE(d.notifications_enabled, TRUE) = TRUE
          AND d.fcm_token IS NOT NULL
          AND BTRIM(d.fcm_token) <> ''
          AND LOWER(BTRIM(d.fcm_token)) <> 'none'
        """,
        (start, end),
    ) or []


def load_state(device_id: str, game_id: int) -> Dict[str, Any]:
    row = nba_fetch_one(
        """
        SELECT last_status, last_home_score, last_away_score, sent_event_keys
        FROM nba_game_notification_states
        WHERE device_id=%s AND game_id=%s
        """,
        (device_id, game_id),
    )
    return row or {
        "last_status": None,
        "last_home_score": None,
        "last_away_score": None,
        "sent_event_keys": [],
    }


def save_state(
    device_id: str,
    game_id: int,
    last_status: Optional[str],
    last_home_score: Optional[int],
    last_away_score: Optional[int],
    sent_event_keys: List[str],
) -> None:
    nba_execute(
        """
        INSERT INTO nba_game_notification_states
          (device_id, game_id, last_status, last_home_score, last_away_score, sent_event_keys, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, now(), now())
        ON CONFLICT (device_id, game_id)
        DO UPDATE SET
          last_status = EXCLUDED.last_status,
          last_home_score = EXCLUDED.last_home_score,
          last_away_score = EXCLUDED.last_away_score,
          sent_event_keys = EXCLUDED.sent_event_keys,
          updated_at = now()
        """,
        (device_id, game_id, last_status, last_home_score, last_away_score, sent_event_keys),
    )


# ─────────────────────────────────────────
# MESSAGE / SEND
# ─────────────────────────────────────────
def _score_line(home_name: str, away_name: str, hs: Optional[int], as_: Optional[int]) -> str:
    hn = home_name or "Home"
    an = away_name or "Away"
    hst = "?" if hs is None else str(hs)
    ast = "?" if as_ is None else str(as_)
    return f"{hn} {hst} : {ast} {an}"


def build_nba_message(phase: Phase, home_name: str, away_name: str, hs: Optional[int], as_: Optional[int]) -> Tuple[str, str]:
    line = _score_line(home_name, away_name, hs, as_)

    if phase.kind == "GAME_START":
        return ("▶ Game Start", f"{home_name} vs {away_name}\n{line}")

    if phase.kind == "Q_START":
        return (f"▶ {phase.index}Q Start", line)

    if phase.kind == "Q_END":
        return (f"⏸ {phase.index}Q End", line)

    if phase.kind == "OT_START":
        return (f"▶ OT{phase.index} Start", line)

    if phase.kind == "OT_END":
        return (f"⏸ OT{phase.index} End", line)

    if phase.kind == "FINAL":
        return ("⏱ Final", line)

    return ("NBA Update", line)


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
# TICK
# ─────────────────────────────────────────
def run_once() -> bool:
    now_utc = datetime.now(timezone.utc)
    rows = fetch_subscription_rows(now_utc)

    if not rows:
        log.info("tick: subs=0 (window=%s/%s min)", LOOKBACK_MIN, LOOKAHEAD_MIN)
        return False

    has_fast = False
    sent = 0

    for r in rows:
        device_id = str(r.get("device_id") or "").strip()
        token = str(r.get("fcm_token") or "").strip()
        game_id = int(r.get("game_id") or 0)
        if not (device_id and token and game_id):
            continue

        notify_game_start = bool(r.get("notify_game_start", True))
        notify_game_end = bool(r.get("notify_game_end", True))
        notify_periods = bool(r.get("notify_periods", True))

        status_short = r.get("status_short")
        status_long = str(r.get("status_long") or "").strip()
        raw = _json_obj(r.get("raw_json"))

        home_name = str(r.get("home_name") or "Home")
        away_name = str(r.get("away_name") or "Away")
        hs, as_ = _extract_scores_from_raw(raw)

        if _is_inplay(status_short, status_long):
            has_fast = True

        st = load_state(device_id, game_id)
        sent_keys: List[str] = list(st.get("sent_event_keys") or [])

        # phase 판정 (쿼터/OT/Final/GameStart)
        phase = _detect_phase(
            status_short=status_short,
            status_long=status_long,
            raw=raw,
            sent_keys=sent_keys,
        )
        if not phase:
            # 스냅샷만 동기화
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            continue

        # 어떤 phase를 보낼지 옵션에 따라 필터
        if phase.kind == "GAME_START" and not notify_game_start:
            # gs 키는 기록해서 이후 OT판정 흐름은 유지(원하면 제거 가능)
            sent_keys.append("gs")
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            continue

        if phase.kind in ("Q_START", "Q_END", "OT_START", "OT_END") and not notify_periods:
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            continue

        if phase.kind == "FINAL" and not notify_game_end:
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            continue

        # event_key (중복 방지)
        if phase.kind == "GAME_START":
            ek = "gs"
        elif phase.kind == "FINAL":
            ek = "final"
        elif phase.kind == "Q_START":
            ek = f"qs:{phase.index}"
        elif phase.kind == "Q_END":
            ek = f"qe:{phase.index}"
        elif phase.kind == "OT_START":
            ek = f"ots:{phase.index}"
        elif phase.kind == "OT_END":
            ek = f"ote:{phase.index}"
        else:
            ek = f"x:{phase.kind}:{phase.index}"

        if ek in sent_keys:
            # 이미 보냈으면 스냅샷만 저장
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            continue

        title, body = build_nba_message(phase, home_name, away_name, hs, as_)

        if send_push(token, title, body, {"sport": "nba", "game_id": str(game_id), "event": ek}):
            sent += 1
            sent_keys.append(ek)

            # 🔒 즉시 저장(하키와 동일)
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            time.sleep(SEND_SLEEP_SEC)
        else:
            # 실패해도 스냅샷 저장은 해두자(다만 ek는 추가하지 않음)
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)

    log.info("tick: sent=%d", sent)
    return has_fast


def run_forever(interval_sec: int) -> None:
    ensure_tables()

    log.info(
        "worker start: window=%s/%s min fast=%ss slow=%ss send_sleep=%ss",
        LOOKBACK_MIN,
        LOOKAHEAD_MIN,
        FAST_INTERVAL_SEC,
        SLOW_INTERVAL_SEC,
        SEND_SLEEP_SEC,
    )

    # ✅ BOOTSTRAP: 재시작 직후 알림 폭탄 방지
    # - states를 현재 스냅샷으로만 동기화
    # - sent_keys는 유지(없으면 빈 배열)
    try:
        now_utc = datetime.now(timezone.utc)
        rows = fetch_subscription_rows(now_utc)
        for r in rows:
            device_id = str(r.get("device_id") or "").strip()
            game_id = int(r.get("game_id") or 0)
            if not (device_id and game_id):
                continue
            raw = _json_obj(r.get("raw_json"))
            hs, as_ = _extract_scores_from_raw(raw)
            status_short = r.get("status_short")
            status_long = str(r.get("status_long") or "").strip()

            st = load_state(device_id, game_id)
            sent_keys: List[str] = list(st.get("sent_event_keys") or [])

            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)

        log.info("bootstrap: synced %d subscription rows (no notifications)", len(rows))
    except Exception:
        log.exception("bootstrap failed (will continue normal loop)")

    while True:
        use_fast = False
        try:
            use_fast = run_once()
        except Exception:
            log.exception("tick failed")

        sleep_sec = max(1, FAST_INTERVAL_SEC) if use_fast else max(1, SLOW_INTERVAL_SEC)
        time.sleep(sleep_sec)


if __name__ == "__main__":
    run_forever(INTERVAL_SEC)

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












def _is_inplay(status_short: Any, status_long: str) -> bool:
    # DB 분포 확정: 2 = In Play
    try:
        return int(status_short) == 2
    except Exception:
        return str(status_long or "").strip().lower() == "in play"




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
    ✅ 추론/예측 금지 버전.
    - 오직 RAW의 확정 필드만 사용:
      raw.status.short (int), raw.periods.current (int), raw.periods.endOfPeriod (bool)
    - 위 필드가 없거나 애매하면 None(=알림 생성 금지, 스냅샷만 저장)
    """

    # RAW 우선
    rs_short = _safe_int(((raw.get("status") or {}).get("short")))
    rs_long = str(((raw.get("status") or {}).get("long")) or "").strip()

    # fallback은 status_short/status_long이 아니라 "FINAL 판단" 정도로만 보조
    ss = rs_short if rs_short is not None else _safe_int(status_short)
    sl = rs_long or str(status_long or "").strip()

    # ✅ FINAL (확정)
    if ss is not None and int(ss) == 3:
        return Phase("FINAL", 0, "Final")
    if str(sl).lower() == "finished":
        # ss가 누락된 드문 케이스 보조
        return Phase("FINAL", 0, "Final")

    # ✅ In Play만 처리 (확정)
    if ss is None:
        # status.short 자체가 없으면 추론 금지
        return None
    if int(ss) != 2:
        return None

    pc = _safe_int(((raw.get("periods") or {}).get("current")))
    eop_val = (raw.get("periods") or {}).get("endOfPeriod")

    # ✅ periods.current / endOfPeriod 둘 중 하나라도 확정이 아니면 추론 금지
    if pc is None or pc < 1:
        return None
    if not isinstance(eop_val, bool):
        return None

    eop = bool(eop_val)

    # ✅ endOfPeriod=True => "End" 이벤트
    if eop:
        if pc <= 4:
            return Phase("Q_END", pc, f"{pc}Q End")
        ot = pc - 4
        return Phase("OT_END", ot, f"OT{ot} End")

    # ✅ endOfPeriod=False => "Start" 이벤트
    # (단, eop가 False일 때만 Start를 인정한다. clock/linescore로 보정하지 않는다.)
    if pc <= 4:
        return Phase("Q_START", pc, f"{pc}Q Start")
    ot = pc - 4
    return Phase("OT_START", ot, f"OT{ot} Start")


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
          s.created_at AS sub_created_at,
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
        SELECT
          last_status,
          last_home_score,
          last_away_score,
          sent_event_keys,
          created_at,
          updated_at
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
        "created_at": None,
        "updated_at": None,
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


def build_nba_message(
    phase: Phase,
    home_name: str,
    away_name: str,
    hs: Optional[int],
    as_: Optional[int],
) -> Tuple[str, str]:
    line = _score_line(home_name, away_name, hs, as_)

    def T(s: str) -> str:
        # 모든 알림 타이틀 앞에 농구공 이모지 고정
        return f"🏀 {s}"

    def B(s: str) -> str:
        # 모든 알림 바디 앞에도 농구공 이모지 고정
        return f"🏀 {s}"

    if phase.kind == "GAME_START":
        return (T("Game started"), B(f"{home_name} vs {away_name}\n{line}"))

    if phase.kind == "Q_START":
        # 🏀 Start of Q1
        return (T(f"Start of Q{phase.index}"), B(line))

    if phase.kind == "Q_END":
        # 🏀 Halftime (end of Q2)
        if phase.index == 2:
            return (T("Halftime"), B(line))
        # 🏀 End of Q1 / Q3 / Q4
        return (T(f"End of Q{phase.index}"), B(line))

    if phase.kind == "OT_START":
        # 🏀 Overtime (OT1), 🏀 Start of OT2 ...
        if phase.index == 1:
            return (T("Overtime"), B(line))
        return (T(f"Start of OT{phase.index}"), B(line))

    if phase.kind == "OT_END":
        # 🏀 End of OT1 ...
        return (T(f"End of OT{phase.index}"), B(line))

    if phase.kind == "FINAL":
        # 🏀 Final
        return (T("Final"), B(line))

    return (T("NBA update"), B(line))


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

        raw_long = str(((raw.get("status") or {}).get("long")) or "").strip()
        raw_short = _safe_int(((raw.get("status") or {}).get("short")))
        eff_long = raw_long or status_long
        eff_short = raw_short if raw_short is not None else status_short

        if _is_inplay(eff_short, eff_long):
            has_fast = True

        # ✅ 먼저 state 로드
        st = load_state(device_id, game_id)
        sent_keys: List[str] = list(st.get("sent_event_keys") or [])

        # ✅ 즐겨찾기(구독) 시각 이후 알림만 보장:
        # - 예전에 구독했다가 해제 후 다시 구독하면 states가 남아있을 수 있다.
        # - 이 경우 구독 created_at이 state.updated_at(또는 created_at)보다 최신이면,
        #   state를 새 구독으로 간주하고 "스냅샷 동기화만" 하도록 리셋한다.
        sub_created_at = r.get("sub_created_at")  # timestamptz
        st_updated_at = st.get("updated_at") or st.get("created_at")

        try:
            if sub_created_at and st_updated_at and sub_created_at > st_updated_at:
                # 새 구독인데 옛 state가 남아있는 상황 -> 리셋(=첫 tick 스냅샷만)
                sent_keys = []
                save_state(device_id, game_id, None, hs, as_, sent_keys)
                continue
        except Exception:
            pass

        # ✅ 구독 직후(last_status가 None인 최초 tick)은 "스냅샷 동기화만" 하고 알림은 보내지 않는다.
        # (중간 구독/워커 재시작 시 과거 단계 알림 폭탄 방지)
        if st.get("last_status") is None:
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            continue

        # phase 판정 (쿼터/OT/Final)
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



        if phase.kind in ("Q_START", "Q_END", "OT_START", "OT_END") and not notify_periods:
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            continue

        if phase.kind == "FINAL" and not notify_game_end:
            save_state(device_id, game_id, status_long or str(status_short), hs, as_, sent_keys)
            continue

        # event_key (중복 방지)
        if phase.kind == "FINAL":
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

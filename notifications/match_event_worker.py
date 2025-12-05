# notifications/match_event_worker.py

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from db import fetch_all, fetch_one, execute
from notifications.fcm_client import FCMClient

log = logging.getLogger("match_event_worker")
logging.basicConfig(level=logging.INFO)

# -------------------------------------------------------
# MatchState dataclass
# -------------------------------------------------------

@dataclass
class MatchState:
    match_id: int
    status: str
    home_goals: int
    away_goals: int
    home_red: int
    away_red: int


# -------------------------------------------------------
# 추가: 경기의 리그명 + 팀명 로딩 함수
# -------------------------------------------------------
def load_match_basic_info(match_id: int):
    row = fetch_one(
        """
        SELECT
            m.fixture_id,
            m.league_id,
            l.name AS league_name,
            h.name AS home_name,
            a.name AS away_name
        FROM matches m
        JOIN leagues l ON l.league_id = m.league_id
        JOIN teams   h ON h.team_id = m.home_id
        JOIN teams   a ON a.team_id = m.away_id
        WHERE m.fixture_id = %s
        """,
        (match_id,)
    )
    return row


# -------------------------------------------------------
# DB → 현재 MatchState 로딩
# -------------------------------------------------------
def get_subscribed_matches() -> List[int]:
    rows = fetch_all(
        """
        SELECT DISTINCT match_id
        FROM match_notification_subscriptions
        """
    )
    return [int(r["match_id"]) for r in rows]


def load_current_match_state(match_id: int) -> MatchState | None:
    row = fetch_one(
        """
        SELECT
            m.fixture_id AS match_id,
            m.status     AS status,
            COALESCE(m.home_ft, 0) AS home_goals,
            COALESCE(m.away_ft, 0) AS away_goals,
            COALESCE((
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.type = 'Card'
                  AND e.detail IN ('Red Card', 'Second Yellow Card')
                  AND e.team_id = m.home_id
            ), 0) AS home_red,
            COALESCE((
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.type = 'Card'
                  AND e.detail IN ('Red Card', 'Second Yellow Card')
                  AND e.team_id = m.away_id
            ), 0) AS away_red
        FROM matches m
        WHERE m.fixture_id = %s
        """,
        (match_id,)
    )

    if not row:
        return None

    return MatchState(
        match_id=int(row["match_id"]),
        status=str(row["status"] or ""),
        home_goals=int(row["home_goals"] or 0),
        away_goals=int(row["away_goals"] or 0),
        home_red=int(row["home_red"] or 0),
        away_red=int(row["away_red"] or 0),
    )


# -------------------------------------------------------
# match_notification_state 저장/로드
# -------------------------------------------------------
def load_last_state(match_id: int) -> MatchState | None:
    row = fetch_one(
        """
        SELECT
            match_id,
            last_status AS status,
            last_home_goals AS home_goals,
            last_away_goals AS away_goals,
            last_home_red AS home_red,
            last_away_red AS away_red
        FROM match_notification_state
        WHERE match_id = %s
        """,
        (match_id,)
    )

    if not row:
        return None

    return MatchState(
        match_id=int(row["match_id"]),
        status=str(row["status"] or ""),
        home_goals=int(row["home_goals"] or 0),
        away_goals=int(row["away_goals"] or 0),
        home_red=int(row["home_red"] or 0),
        away_red=int(row["away_red"] or 0),
    )


def save_state(state: MatchState) -> None:
    execute(
        """
        INSERT INTO match_notification_state (
            match_id,
            last_status,
            last_home_goals,
            last_away_goals,
            last_home_red,
            last_away_red,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (match_id)
        DO UPDATE SET
            last_status     = EXCLUDED.last_status,
            last_home_goals = EXCLUDED.last_home_goals,
            last_away_goals = EXCLUDED.last_away_goals,
            last_home_red   = EXCLUDED.last_home_red,
            last_away_red   = EXCLUDED.last_away_red,
            updated_at      = NOW();
        """,
        (
            state.match_id,
            state.status,
            state.home_goals,
            state.away_goals,
            state.home_red,
            state.away_red,
        ),
    )


# -------------------------------------------------------
# diff_events — 상태 변화 감지(개선 버전)
# -------------------------------------------------------
def diff_events(old: MatchState | None, new: MatchState) -> List[Tuple[str, Dict[str, Any]]]:
    events = []

    if old is None:
        return events

    old_s = old.status or ""
    new_s = new.status or ""

    # Kickoff
    if old_s in ("NS", "TBD", "") and new_s in ("1H", "LIVE"):
        events.append(("kickoff", {}))

    # Half-time
    if new_s == "HT" and old_s != "HT":
        events.append(("ht", {}))

    # Second half start
    if old_s == "HT" and new_s in ("2H", "LIVE"):
        events.append(("second_half", {}))

    # Full-time
    if new_s in ("FT", "AET", "PEN") and old_s not in ("FT", "AET", "PEN"):
        events.append(("ft", {}))

    # Score change
    if (old.home_goals != new.home_goals) or (old.away_goals != new.away_goals):
        events.append((
            "score",
            {
                "old_home": old.home_goals,
                "old_away": old.away_goals,
                "new_home": new.home_goals,
                "new_away": new.away_goals,
            }
        ))

    # Red card
    if (old.home_red != new.home_red) or (old.away_red != new.away_red):
        events.append((
            "redcard",
            {
                "old_home": old.home_red,
                "old_away": old.away_red,
                "new_home": new.home_red,
                "new_away": new.away_red,
            }
        ))

    return events


# -------------------------------------------------------
# 구독자 FCM 토큰 가져오기 (옵션 분리됨)
# -------------------------------------------------------
def get_tokens_for_event(match_id: int, event_type: str) -> List[str]:
    option_column = {
        "kickoff": "notify_kickoff",
        "ht": "notify_ht",
        "second_half": "notify_second_half",
        "score": "notify_score",
        "redcard": "notify_redcard",
        "ft": "notify_ft",
    }[event_type]

    rows = fetch_all(
        f"""
        SELECT u.fcm_token
        FROM match_notification_subscriptions s
        JOIN user_devices u ON u.device_id = s.device_id
        WHERE s.match_id = %s
          AND s.{option_column} = TRUE
          AND u.notifications_enabled = TRUE
        """,
        (match_id,)
    )

    return [str(r["fcm_token"]) for r in rows]


# -------------------------------------------------------
# 알림 메시지 생성(팀/리그 이름 포함)
# -------------------------------------------------------
def build_message(event_type: str, match: MatchState, extra: Dict[str, Any],
                  league_name: str, home_team: str, away_team: str) -> Tuple[str, str]:

    score_str = f"{match.home_goals} : {match.away_goals}"

    if event_type == "kickoff":
        return (f"[{league_name}] {home_team} vs {away_team}",
                "경기가 시작되었습니다.")

    if event_type == "ht":
        return (f"[{league_name}] {home_team} vs {away_team}",
                f"전반 종료 — 스코어 {score_str}")

    if event_type == "second_half":
        return (f"[{league_name}] {home_team} vs {away_team}",
                "후반전이 시작되었습니다.")

    if event_type == "ft":
        return (f"[{league_name}] {home_team} vs {away_team}",
                f"경기 종료 — 최종 스코어 {score_str}")

    if event_type == "score":
        team = home_team if extra["new_home"] > extra["old_home"] else away_team
        return (f"[{league_name}] 득점! {home_team} vs {away_team}",
                f"{team} 득점!\n현재 스코어 {score_str}")

    if event_type == "redcard":
        return (f"[{league_name}] 레드카드 — {home_team} vs {away_team}",
                "레드카드 발생")

    return ("Match update", f"스코어 {score_str}")


# -------------------------------------------------------
# match 처리
# -------------------------------------------------------
def process_match(fcm: FCMClient, match_id: int) -> None:
    current = load_current_match_state(match_id)
    if not current:
        log.info("match_id=%s current state not found", match_id)
        return

    last = load_last_state(match_id)
    events = diff_events(last, current)

    if not events:
        save_state(current)
        return

    # 추가: 경기 기본 정보 로딩 (리그명/팀명)
    info = load_match_basic_info(match_id)
    league_name = info["league_name"]
    home_team = info["home_name"]
    away_team = info["away_name"]

    # 이벤트 반복 처리
    for event_type, extra in events:
        tokens = get_tokens_for_event(match_id, event_type)
        if not tokens:
            continue

        title, body = build_message(
            event_type,
            current,
            extra,
            league_name,
            home_team,
            away_team
        )

        data = {"match_id": match_id, "event_type": event_type}
        data.update(extra)

        # 500개 묶음으로 FCM 발송
        batch_size = 500
        for i in range(0, len(tokens), batch_size):
            batch = tokens[i : i + batch_size]
            try:
                resp = fcm.send_to_tokens(batch, title, body, data)
                log.info("Sent %s for match %s (%s devices): %s",
                         event_type, match_id, len(batch), resp)
            except Exception:
                log.exception("Failed sending %s for %s", event_type, match_id)

    save_state(current)


# -------------------------------------------------------
# run loop
# -------------------------------------------------------
def run_once(fcm: FCMClient | None = None) -> None:
    if fcm is None:
        fcm = FCMClient()

    matches = get_subscribed_matches()
    if not matches:
        log.info("No subscribed matches.")
        return

    log.info("Processing %d subscribed matches...", len(matches))

    for match_id in matches:
        process_match(fcm, match_id)


def run_forever(interval_seconds: int = 10) -> None:
    fcm = FCMClient()
    log.info("Starting worker (interval=%s sec)", interval_seconds)

    while True:
        try:
            run_once(fcm)
        except Exception:
            log.exception("Worker loop error")

        time.sleep(interval_seconds)


if __name__ == "__main__":
    interval = os.getenv("MATCH_WORKER_INTERVAL_SEC")

    if interval:
        try:
            seconds = int(interval)
        except ValueError:
            seconds = 10
        run_forever(seconds)
    else:
        run_once()

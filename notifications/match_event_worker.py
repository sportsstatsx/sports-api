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


@dataclass
class MatchState:
    match_id: int
    status: str  # 예: 'NS', '1H', 'HT', '2H', 'FT'
    home_goals: int
    away_goals: int
    home_red: int
    away_red: int


def get_subscribed_matches() -> List[int]:
    rows = fetch_all(
        """
        SELECT DISTINCT match_id
        FROM match_notification_subscriptions
        """
    )
    return [int(r["match_id"]) for r in rows]


def load_current_match_state(match_id: int) -> MatchState | None:
    """
    현재 match_id 경기의 상태를 DB에서 읽어서 MatchState로 반환한다.

    - 골 수는 matches.home_ft / matches.away_ft 사용
    - 레드카드는 match_events 에서 type='Card' + detail 이 레드카드인 이벤트를
      홈/원정팀별로 COUNT 해서 계산
    """
    row = fetch_one(
        """
        SELECT
            m.fixture_id AS match_id,
            m.status     AS status,
            COALESCE(m.home_ft, 0) AS home_goals,
            COALESCE(m.away_ft, 0) AS away_goals,
            COALESCE(
                (
                    SELECT COUNT(*)
                    FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.type = 'Card'
                      AND e.detail IN ('Red Card', 'Second Yellow Card')
                      AND e.team_id = m.home_id
                ),
                0
            ) AS home_red,
            COALESCE(
                (
                    SELECT COUNT(*)
                    FROM match_events e
                    WHERE e.fixture_id = m.fixture_id
                      AND e.type = 'Card'
                      AND e.detail IN ('Red Card', 'Second Yellow Card')
                      AND e.team_id = m.away_id
                ),
                0
            ) AS away_red
        FROM matches m
        WHERE m.fixture_id = %s
        """,
        (match_id,),
    )

    if not row:
        # 해당 match_id 경기 자체가 없으면 None
        return None

    return MatchState(
        match_id=int(row["match_id"]),
        status=str(row["status"]) if row["status"] is not None else "",
        home_goals=int(row["home_goals"] or 0),
        away_goals=int(row["away_goals"] or 0),
        home_red=int(row["home_red"] or 0),
        away_red=int(row["away_red"] or 0),
    )



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
        (match_id,),
    )
    if not row:
        return None

    return MatchState(
        match_id=int(row["match_id"]),
        status=str(row["status"]) if row["status"] is not None else "",
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


def diff_events(old: MatchState | None, new: MatchState) -> List[Tuple[str, Dict[str, Any]]]:
    """
    상태 변화에서 어떤 이벤트가 발생했는지 계산.
    리턴: [(event_type, extra_data), ...]
      event_type: 'kickoff' | 'score' | 'redcard' | 'ht' | '2h_start' | 'ft'
    """
    events: List[Tuple[str, Dict[str, Any]]] = []

    if old is None:
        # 처음 상태 저장만 하고, 이벤트는 안 보냄
        return events

    # status 변경
    if old.status != new.status:
        if old.status in ("NS", "TBD") and new.status in ("1H", "LIVE"):
            events.append(("kickoff", {}))
        elif new.status == "HT":
            events.append(("ht", {}))
        elif old.status == "HT" and new.status in ("2H", "LIVE"):
            events.append(("2h_start", {}))
        elif new.status in ("FT", "AET", "PEN"):
            events.append(("ft", {}))

    # 스코어 변경
    if (old.home_goals, old.away_goals) != (new.home_goals, new.away_goals):
        events.append(
            (
                "score",
                {
                    "old_home": old.home_goals,
                    "old_away": old.away_goals,
                    "new_home": new.home_goals,
                    "new_away": new.away_goals,
                },
            )
        )

    # 레드카드 변경
    if (old.home_red, old.away_red) != (new.home_red, new.away_red):
        events.append(
            (
                "redcard",
                {
                    "old_home": old.home_red,
                    "old_away": old.away_red,
                    "new_home": new.home_red,
                    "new_away": new.away_red,
                },
            )
        )

    return events


def get_tokens_for_event(match_id: int, event_type: str) -> List[str]:
    """
    이벤트 종류에 따라 해당 옵션을 켜둔 구독자 토큰만 가져오기.
    """
    option_column = {
        "kickoff": "notify_kickoff",
        "score": "notify_score",
        "redcard": "notify_redcard",
        "ht": "notify_kickoff",   # HT/2H_start 는 kickoff 옵션에 묶을지, 필요하면 따로 분리
        "2h_start": "notify_kickoff",
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
        (match_id,),
    )

    return [str(r["fcm_token"]) for r in rows]


def build_message(event_type: str, match: MatchState, extra: Dict[str, Any]) -> Tuple[str, str]:
    """
    이벤트별 FCM 제목/내용 문자열.
    나중에 실제 팀 이름, 리그 이름까지 넣고 싶으면 쿼리 추가해서 확장하면 됨.
    """
    score_str = f"{match.home_goals} - {match.away_goals}"

    if event_type == "kickoff":
        return ("Kickoff", f"경기가 시작되었습니다. (현재 스코어 {score_str})")
    if event_type == "ht":
        return ("Half-time", f"전반 종료: 스코어 {score_str}")
    if event_type == "2h_start":
        return ("Second half", "후반전이 시작되었습니다.")
    if event_type == "ft":
        return ("Full-time", f"경기 종료: 최종 스코어 {score_str}")
    if event_type == "score":
        return ("Goal!", f"득점 발생: 스코어 {score_str}")
    if event_type == "redcard":
        return ("Red card", f"레드카드 발생: 스코어 {score_str}")

    return ("Match update", f"경기 업데이트: 스코어 {score_str}")


def process_match(fcm: FCMClient, match_id: int) -> None:
    current = load_current_match_state(match_id)
    if not current:
        log.info("match_id=%s current state not found, skip", match_id)
        return

    last = load_last_state(match_id)
    events = diff_events(last, current)

    if not events:
        # 변화 없음 → 상태만 저장
        save_state(current)
        return

    for event_type, extra in events:
        tokens = get_tokens_for_event(match_id, event_type)
        if not tokens:
            continue

        title, body = build_message(event_type, current, extra)
        data = {
            "match_id": match_id,
            "event_type": event_type,
        }
        data.update(extra)

        # 너무 많이 쏘지 않도록 500개 단위로 잘라서 발송
        batch_size = 500
        for i in range(0, len(tokens), batch_size):
            batch = tokens[i : i + batch_size]
            try:
                resp = fcm.send_to_tokens(batch, title, body, data)
                log.info(
                    "Sent %s notification for match %s to %s devices: %s",
                    event_type,
                    match_id,
                    len(batch),
                    resp,
                )
            except Exception as e:
                log.exception("Failed to send %s notification for match %s", event_type, match_id)

    # 모든 이벤트 처리 후 상태를 최신으로 업데이트
    save_state(current)


def run_once(fcm: FCMClient | None = None) -> None:
    """
    기존 main() 과 동일하게 한 번만 돌면서
    즐겨찾기된 경기들의 변화만 체크해서 푸시를 보냄.
    """
    if fcm is None:
        fcm = FCMClient()

    matches = get_subscribed_matches()
    if not matches:
        log.info("No subscribed matches, nothing to do.")
        return

    log.info("Processing %s subscribed matches...", len(matches))
    for match_id in matches:
        process_match(fcm, match_id)


def run_forever(interval_seconds: int = 10) -> None:
    """
    Worker 모드: interval_seconds 간격으로 run_once 를 반복 실행.
    """
    fcm = FCMClient()
    log.info(
        "Starting match_event_worker in worker mode (interval=%s sec)",
        interval_seconds,
    )

    while True:
        try:
            run_once(fcm)
        except Exception:
            # 에러가 나도 워커가 죽지 않도록 로그만 찍고 다음 루프로 진행
            log.exception("Error while processing matches in worker loop")

        time.sleep(interval_seconds)


if __name__ == "__main__":
    # 환경변수 MATCH_WORKER_INTERVAL_SEC 이 설정되어 있으면
    # 그 값을 초 단위로 사용해서 worker 모드로 실행.
    # 없으면 예전처럼 한 번만 실행하고 종료(run_once).
    interval = os.getenv("MATCH_WORKER_INTERVAL_SEC")

    if interval:
        try:
            seconds = int(interval)
        except ValueError:
            seconds = 10  # 잘못된 값이면 기본 10초
        run_forever(seconds)
    else:
        run_once()


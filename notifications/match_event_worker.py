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


# 상태 진행 순서를 숫자로 매핑해서 "뒤로 가는 롤백"을 막기 위한 기준
STATUS_ORDER: Dict[str, int] = {
    "": 0,
    "TBD": 0,
    "NS": 0,
    "PST": 1,   # 연기
    "SUSP": 1,  # 중단
    "1H": 10,
    "LIVE": 15,  # 공급사에서 LIVE 로만 주는 경우 방지용
    "HT": 20,
    "2H": 30,
    "ET": 40,
    "P": 50,
    "AET": 60,
    "PEN": 70,
    "FT": 80,
}


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


def load_match_labels(match_id: int) -> Dict[str, str]:
    """
    팀 이름(필수), 리그 이름(옵션)을 한 번에 가져오는 헬퍼.
    알림 메시지에서 사용한다.
    """
    row = fetch_one(
        """
        SELECT
            m.fixture_id AS match_id,
            COALESCE(th.name, 'Home') AS home_name,
            COALESCE(ta.name, 'Away') AS away_name,
            COALESCE(l.name, '')      AS league_name
        FROM matches m
        LEFT JOIN teams   th ON th.id = m.home_id
        LEFT JOIN teams   ta ON ta.id = m.away_id
        LEFT JOIN leagues l  ON l.id = m.league_id
        WHERE m.fixture_id = %s
        """,
        (match_id,),
    )

    if not row:
        # 최소한 기본값이라도 리턴
        return {
            "home_name": "Home",
            "away_name": "Away",
            "league_name": "",
        }

    return {
        "home_name": str(row["home_name"]),
        "away_name": str(row["away_name"]),
        "league_name": str(row["league_name"] or ""),
    }


def load_last_goal_minute(match_id: int) -> Dict[str, int] | None:
    """
    마지막 득점 이벤트의 시간(분 + 추가시간)을 가져오는 헬퍼.
    - match_events 에서 type='Goal' 인 것만 대상으로,
      분 내림차순 + extra 내림차순 + id 내림차순으로 한 개만 가져온다.
    """
    row = fetch_one(
        """
        SELECT
            minute,
            COALESCE(extra, 0) AS extra
        FROM match_events
        WHERE fixture_id = %s
          AND type = 'Goal'
        ORDER BY minute DESC NULLS LAST,
                 extra DESC NULLS LAST,
                 id DESC
        LIMIT 1
        """,
        (match_id,),
    )

    if not row or row["minute"] is None:
        return None

    return {
        "minute": int(row["minute"]),
        "extra": int(row["extra"] or 0),
    }


def apply_monotonic_state(
    last: MatchState | None,
    current: MatchState,
) -> MatchState:
    """
    match_notification_state 를 기준으로
    - status 는 STATUS_ORDER 를 기준으로 뒤로 가지 않게
    - 골 / 레드카드는 절대 감소하지 않게
    만든 "유효 상태"를 만들어낸다.

    이렇게 만들어진 상태를 기준으로 diff 를 계산하고,
    같은 값을 match_notification_state 에 저장해서
    이후에도 항상 단조롭게 유지한다.
    """
    if last is None:
        return current

    old_status = last.status or ""
    new_status = current.status or ""

    old_rank = STATUS_ORDER.get(old_status, 0)
    new_rank = STATUS_ORDER.get(new_status, 0)

    # 상태가 뒤로 가면(랭크가 작아지면) 이전 상태를 유지
    if new_rank < old_rank:
        effective_status = old_status
    else:
        effective_status = new_status

    return MatchState(
        match_id=current.match_id,
        status=effective_status,
        home_goals=max(last.home_goals, current.home_goals),
        away_goals=max(last.away_goals, current.away_goals),
        home_red=max(last.home_red, current.home_red),
        away_red=max(last.away_red, current.away_red),
    )


def diff_events(old: MatchState | None, new: MatchState) -> List[Tuple[str, Dict[str, Any]]]:
    events: List[Tuple[str, Dict[str, Any]]] = []

    # 첫 상태 저장용 (알림 X)
    if old is None:
        return events

    old_status = old.status or ""
    new_status = new.status or ""

    # ✅ 이미 종료 알림 보낸 경기면 아무것도 안 함
    if old_status in ("FT", "AET", "PEN"):
        return events

    # ==========================
    # 1) Kickoff (완화된 기준)
    # ==========================
    if old_status in ("", "NS", "TBD") and new_status not in ("", "NS", "TBD"):
        events.append(("kickoff", {}))

    # ==========================
    # 2) Half-time
    # ==========================
    if new_status == "HT" and old_status != "HT":
        events.append(("ht", {}))

    # ==========================
    # 3) Second half start
    # ==========================
    if old_status == "HT" and new_status in ("2H", "LIVE"):
        events.append(("2h_start", {}))

    # ==========================
    # 4) Full-time
    # ==========================
    if new_status in ("FT", "AET", "PEN") and old_status not in ("FT", "AET", "PEN"):
        events.append(("ft", {}))

    # ==========================
    # 5) Goal (증가만 감지)
    # ==========================
    if new.home_goals > old.home_goals or new.away_goals > old.away_goals:
        events.append(
            (
                "score",
                {
                    "old_home": old.home_goals,
                    "old_away": old.away_goals,
                },
            )
        )

    # ==========================
    # 6) Red card (증가만 감지)
    # ==========================
    if new.home_red > old.home_red or new.away_red > old.away_red:
        events.append(
            (
                "redcard",
                {
                    "old_home": old.home_red,
                    "old_away": old.away_red,
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
        "ht": "notify_ht",          # 하프타임 전용 옵션
        "2h_start": "notify_2h",    # 후반 시작 전용 옵션
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


def build_message(
    event_type: str,
    match: MatchState,
    extra: Dict[str, Any],
    labels: Dict[str, str],
) -> Tuple[str, str]:
    """
    이벤트별 FCM 제목/내용 문자열을 생성한다.
    - 글로벌(미국식) 영어 스타일
    - 리그 이름은 문구에서 제외 (요청 사항)
    - 득점/레드카드에는 팀 이름 + 이모지 포함
    - HT/2H/FT 는 타이틀 한 줄 + 바디에 스코어
    """
    home_name = labels.get("home_name", "Home")
    away_name = labels.get("away_name", "Away")

    # en dash 사용
    score_line = f"{home_name} {match.home_goals}–{match.away_goals} {away_name}"

    # Kickoff
    if event_type == "kickoff":
        title = "Kickoff"
        body = f"{home_name} vs {away_name}"
        return (title, body)

    # Half-time
    if event_type == "ht":
        title = "— Half-time —"
        body = score_line
        return (title, body)

    # Second half start
    if event_type == "2h_start":
        title = "— Second Half —"
        body = score_line
        return (title, body)

    # Full-time
    if event_type == "ft":
        title = "— Full-time —"
        body = score_line
        return (title, body)

    # Goal (score)
    if event_type == "score":
        old_home = int(extra.get("old_home", match.home_goals))
        old_away = int(extra.get("old_away", match.away_goals))
        new_home = match.home_goals
        new_away = match.away_goals

        # 어느 팀이 득점했는지 판별
        if (new_home > old_home) and (new_away == old_away):
            scorer_team = home_name
        elif (new_away > old_away) and (new_home == old_home):
            scorer_team = away_name
        else:
            # 동시에 2골 이상 업데이트되거나 애매한 상황 → 중립 문구
            scorer_team = "Goal"

        # process_match 에서 넣어준 시간 문자열
        goal_minute_str = extra.get("goal_minute_str")

        # 타이틀 포맷: "Liverpool Goal! ⚽ 67'"
        if scorer_team in (home_name, away_name):
            if goal_minute_str:
                title = f"{scorer_team} Goal! ⚽ {goal_minute_str}"
            else:
                title = f"{scorer_team} Goal! ⚽"
        else:
            if goal_minute_str:
                title = f"Goal! ⚽ {goal_minute_str}"
            else:
                title = "Goal! ⚽"

        body = score_line
        return (title, body)

    # Red card
    if event_type == "redcard":
        old_home_red = int(extra.get("old_home", match.home_red))
        old_away_red = int(extra.get("old_away", match.away_red))
        new_home_red = match.home_red
        new_away_red = match.away_red

        if (new_home_red > old_home_red) and (new_away_red == old_away_red):
            red_team = home_name
        elif (new_away_red > old_away_red) and (new_home_red == old_home_red):
            red_team = away_name
        else:
            red_team = "Red Card"

        if red_team in (home_name, away_name):
            title = f"{red_team} Red Card! 🟥"
        else:
            title = "Red Card! 🟥"

        body = score_line
        return (title, body)

    # Fallback
    title = "Match update"
    body = score_line
    return (title, body)


def process_match(fcm: FCMClient, match_id: int) -> None:
    current_raw = load_current_match_state(match_id)
    if not current_raw:
        log.info("match_id=%s current state not found, skip", match_id)
        return

    last = load_last_state(match_id)

    # match_notification_state 를 기준으로 단조 상태 강제
    current = apply_monotonic_state(last, current_raw)

    events = diff_events(last, current)

    if not events:
        # 변화 없음 → 상태만 저장
        save_state(current)
        return

    # 팀/리그 이름 라벨을 한 번만 로딩해서 여러 이벤트에 사용
    labels = load_match_labels(match_id)

    for event_type, extra in events:
        # extra(튜플에서 온 dict-like)를 방어적으로 복사
        extra = dict(extra)

        # score 이벤트라면, 마지막 득점 시간(분+추가시간)을 extra 에 추가
        if event_type == "score":
            goal_time = load_last_goal_minute(match_id)
            if goal_time:
                minute = goal_time.get("minute", 0)
                extra_min = goal_time.get("extra", 0) or 0

                if extra_min:
                    # 예: 45+2'
                    goal_minute_str = f"{minute}+{extra_min}'"
                else:
                    # 예: 67'
                    goal_minute_str = f"{minute}'"

                extra["goal_minute_str"] = goal_minute_str

        tokens = get_tokens_for_event(match_id, event_type)
        if not tokens:
            continue

        title, body = build_message(event_type, current, extra, labels)
        data: Dict[str, Any] = {
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
            except Exception:
                log.exception(
                    "Failed to send %s notification for match %s",
                    event_type,
                    match_id,
                )

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

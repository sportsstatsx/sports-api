# notifications/match_event_worker.py

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
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


def load_match_labels(match_id: int) -> Dict[str, Any]:
    """
    팀 이름(필수), 리그 이름(옵션), 홈/원정 team_id 를 한 번에 가져오는 헬퍼.
    알림 메시지 및 팀 판별(Goal Disallowed 등)에서 사용한다.
    """
    row = fetch_one(
        """
        SELECT
            m.fixture_id AS match_id,
            m.home_id    AS home_id,
            m.away_id    AS away_id,
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
        return {
            "home_id": None,
            "away_id": None,
            "home_name": "Home",
            "away_name": "Away",
            "league_name": "",
        }

    return {
        "home_id": int(row["home_id"]) if row["home_id"] is not None else None,
        "away_id": int(row["away_id"]) if row["away_id"] is not None else None,
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

def load_last_redcard_minute(match_id: int) -> Dict[str, int] | None:
    """
    마지막 레드카드 이벤트의 시간(분 + 추가시간)을 가져오는 헬퍼.
    - match_events 에서 type='Card'
      AND detail IN ('Red Card', 'Second Yellow Card') 인 것만 대상으로,
      분 내림차순 + extra 내림차순 + id 내림차순으로 한 개만 가져온다.
    """
    row = fetch_one(
        """
        SELECT
            minute,
            COALESCE(extra, 0) AS extra
        FROM match_events
        WHERE fixture_id = %s
          AND type = 'Card'
          AND detail IN ('Red Card', 'Second Yellow Card')
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


def load_new_goal_disallowed_events(match_id: int, last_event_id: int) -> List[Dict[str, Any]]:
    """
    VAR 'Goal Disallowed%' 이벤트 중 아직 처리하지 않은(= id > last_event_id) 것만 가져온다.
    id ASC 로 정렬해서 발생 순서대로 처리.
    """
    rows = fetch_all(
        """
        SELECT
            id,
            team_id,
            minute,
            COALESCE(extra, 0) AS extra,
            detail
        FROM match_events
        WHERE fixture_id = %s
          AND type = 'Var'
          AND detail ILIKE 'Goal Disallowed%%'
          AND id > %s
        ORDER BY id ASC
        """,
        (match_id, last_event_id),
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "team_id": int(r["team_id"]) if r.get("team_id") is not None else None,
                "minute": int(r["minute"]) if r.get("minute") is not None else 0,
                "extra": int(r["extra"] or 0),
                "detail": str(r["detail"] or ""),
            }
        )
    return out




def apply_monotonic_state(
    last: MatchState | None,
    current: MatchState,
    *,
    allow_goal_decrease: bool = False,
) -> MatchState:
    if last is None:
        return current

    old_status = last.status or ""
    new_status = current.status or ""

    old_rank = STATUS_ORDER.get(old_status, 0)
    new_rank = STATUS_ORDER.get(new_status, 0)

    if new_rank < old_rank:
        effective_status = old_status
    else:
        effective_status = new_status

    if allow_goal_decrease:
        eff_home_goals = current.home_goals
        eff_away_goals = current.away_goals
    else:
        eff_home_goals = max(last.home_goals, current.home_goals)
        eff_away_goals = max(last.away_goals, current.away_goals)

    return MatchState(
        match_id=current.match_id,
        status=effective_status,
        home_goals=eff_home_goals,
        away_goals=eff_away_goals,
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

    # ✅ 이미 진짜로 끝난 경기(FT/AET)이면 아무 것도 안 함
    # PEN 은 여기서 제외해야 PEN → FT/AET 전환 시 알림을 보낼 수 있음
    if old_status in ("FT", "AET"):
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
    # 4) 연장 / 승부차기 / 최종 종료 흐름
    #
    # 의도한 플로우
    #  - 2H → FT        → ft만
    #  - 2H → ET        → et_start만
    #  - ET → AET       → et_end → ft
    #  - ET → PEN       → et_end → pen_start
    #  - PEN → FT/AET   → pen_end → ft
    # ==========================

    # 4-1) 2H(또는 기타) → ET : 연장 시작
    if old_status not in ("ET", "AET", "P", "PEN") and new_status == "ET":
        events.append(("et_start", {}))

    # 4-2) ET → AET/FT : 연장 종료 + 최종 종료
    if old_status == "ET" and new_status in ("AET", "FT"):
        events.append(("et_end", {}))
        events.append(("ft", {}))

    # 4-3) ET → P/PEN : 연장 종료 + 승부차기 시작
    if old_status == "ET" and new_status in ("P", "PEN"):
        events.append(("et_end", {}))
        events.append(("pen_start", {}))

    # 4-4) P/PEN → FT/AET : 승부차기 종료 + 최종 종료
    if old_status in ("P", "PEN") and new_status in ("FT", "AET"):
        events.append(("pen_end", {}))
        events.append(("ft", {}))

    # 4-5) 연장/승부차기 없이 바로 끝나는 경기:
    #      위 케이스들에서 아무 이벤트도 안 쌓인 상태에서 FT/AET 가 되면 ft 1번만 보냄
    if new_status in ("FT", "AET") and not events:
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
        # 킥오프 관련
        "kickoff_10m": "notify_kickoff",  # 🔹 킥오프 10분 전
        "kickoff": "notify_kickoff",

        # 득점 / 카드
        "score": "notify_score",
        "goal_disallowed": "notify_score",  # ✅ 골 무효(VAR)도 득점 알림 옵션에 묶음
        "redcard": "notify_redcard",

        # 전/후반
        "ht": "notify_ht",          # 하프타임 전용 옵션
        "2h_start": "notify_2h",    # 후반 시작 전용 옵션

        # 경기 종료 및 연장/승부차기 관련
        "ft": "notify_ft",
        "et_start": "notify_ft",    # 연장도 일단 FT 알림 옵션에 묶기
        "et_end": "notify_ft",
        "pen_start": "notify_ft",
        "pen_end": "notify_ft",
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
        title = "▶ Kickoff"
        body = f"{home_name} vs {away_name}"
        return (title, body)

    # Half-time
    if event_type == "ht":
        title = "⏸ Half-time"
        body = score_line
        return (title, body)

    # Second half start
    if event_type == "2h_start":
        title = "▶ Second Half"
        body = score_line
        return (title, body)

    # Full-time
    if event_type == "ft":
        title = "⏱ Full-time"
        body = score_line
        return (title, body)

    # Extra time start
    if event_type == "et_start":
        title = "▶ Extra Time"
        body = score_line
        return (title, body)

    # Extra time end
    if event_type == "et_end":
        title = "⏱ Extra Time End"
        body = score_line
        return (title, body)

    # Penalty shoot-out start
    if event_type == "pen_start":
        title = "🥅 Penalties"
        body = score_line
        return (title, body)

    # Penalty shoot-out end
    if event_type == "pen_end":
        title = "⏱ Penalties End"
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
            # 항상 이모지 먼저
            if goal_minute_str:
                title = f"⚽ {goal_minute_str} {scorer_team} Goal!"
            else:
                title = f"⚽ {scorer_team} Goal!"
        else:
            if goal_minute_str:
                title = f"⚽ {goal_minute_str} Goal!"
            else:
                title = "⚽ Goal!"

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

        # 득점처럼 레드카드 시간 문자열 사용
        red_minute_str = extra.get("red_minute_str")

        # 🔥 최종 포맷 예시:
        # 🟥 78' Liverpool Red Card!
        if red_team in (home_name, away_name):
            if red_minute_str:
                title = f"🟥 {red_minute_str} {red_team} Red Card!"
            else:
                title = f"🟥 {red_team} Red Card!"
        else:
            if red_minute_str:
                title = f"🟥 {red_minute_str} Red Card!"
            else:
                title = "🟥 Red Card!"

        body = score_line
        return (title, body)

    # Goal disallowed (VAR)
    if event_type == "goal_disallowed":
        dis_minute_str = extra.get("disallowed_minute_str")
        dis_team = extra.get("disallowed_team")
        dis_reason = extra.get("disallowed_reason")

        # 예: 🚫 45+2' West Ham Goal Disallowed (Offside)
        parts: List[str] = []
        if dis_minute_str:
            parts.append(dis_minute_str)
        if dis_team:
            parts.append(dis_team)

        base = "Goal Disallowed"
        if dis_reason:
            base = f"{base} ({dis_reason})"

        if parts:
            title = f"🚫 {' '.join(parts)} {base}"
        else:
            title = f"🚫 {base}"

        body = score_line
        return (title, body)




    # Fallback
    title = "Match update"
    body = score_line
    return (title, body)


def maybe_send_kickoff_10m(fcm: FCMClient, match: MatchState) -> None:
    """
    킥오프 10분 전 알림:
    - status 가 아직 NS/TBD 일 때만
    - match_notification_state.kickoff_10m_sent 가 FALSE 일 때만
    - date_utc 기준으로 지금 시각과의 차이가 0~600초(10분) 사이면 발송
    """
    # 이미 시작한 경기면 10분 전 알림은 의미 없음
    if match.status not in ("", "NS", "TBD"):
        return

    # 경기 킥오프 시간 가져오기
    row = fetch_one(
        """
        SELECT date_utc
        FROM matches
        WHERE fixture_id = %s
        """,
        (match.match_id,),
    )
    if not row or not row["date_utc"]:
        return

    try:
        # 예: "2025-12-11T17:45:00+00:00"
        kickoff_dt = datetime.fromisoformat(str(row["date_utc"]))
    except Exception:
        return

    now_utc = datetime.now(timezone.utc)
    diff_sec = (kickoff_dt - now_utc).total_seconds()

    # 지금 시각 기준으로 0~600초(10분) 이내만 허용
    if not (0 <= diff_sec <= 600):
        return

    # 이미 10분 전 알림을 보냈는지 확인
    state_row = fetch_one(
        """
        SELECT kickoff_10m_sent
        FROM match_notification_state
        WHERE match_id = %s
        """,
        (match.match_id,),
    )
    if not state_row:
        # 아직 state row 없는 경우엔 스킵 (다음 루프에서 다시 확인)
        return

    if state_row["kickoff_10m_sent"]:
        return

    # 구독 토큰 가져오기 (킥오프와 동일 옵션 사용)
    tokens = get_tokens_for_event(match.match_id, "kickoff_10m")
    if not tokens:
        return

    labels = load_match_labels(match.match_id)
    home_name = labels.get("home_name", "Home")
    away_name = labels.get("away_name", "Away")

    title = "Kickoff in 10 minutes"
    body = f"{home_name} vs {away_name}"

    data: Dict[str, Any] = {
        "match_id": match.match_id,
        "event_type": "kickoff_10m",
    }

    # 500개 단위로 잘라서 발송
    batch_size = 500
    for i in range(0, len(tokens), batch_size):
        batch = tokens[i : i + batch_size]
        try:
            resp = fcm.send_to_tokens(batch, title, body, data)
            log.info(
                "Sent kickoff_10m notification for match %s to %s devices: %s",
                match.match_id,
                len(batch),
                resp,
            )
        except Exception:
            log.exception(
                "Failed to send kickoff_10m notification for match %s",
                match.match_id,
            )

    # 플래그 ON
    execute(
        """
        UPDATE match_notification_state
        SET kickoff_10m_sent = TRUE,
            updated_at = NOW()
        WHERE match_id = %s
        """,
        (match.match_id,),
    )


def process_match(fcm: FCMClient, match_id: int) -> None:
    current_raw = load_current_match_state(match_id)
    if not current_raw:
        log.info("match_id=%s current state not found, skip", match_id)
        return

    last = load_last_state(match_id)

    # ✅ state row 존재 여부(먼저 확인: last_goal_disallowed_event_id 조회 안정)
    state_exists = fetch_one(
        """
        SELECT 1 AS ok
        FROM match_notification_state
        WHERE match_id = %s
        """,
        (match_id,),
    )

    # ✅ state row가 없으면 먼저 생성 + VAR 포인터 초기화(과거 이벤트 폭탄 방지)
    if not state_exists:
        # 첫 진입은 raw 기준으로 저장(기본값 컬럼들도 함께 생김)
        save_state(current_raw)

        mx = fetch_one(
            """
            SELECT COALESCE(MAX(id), 0) AS max_id
            FROM match_events
            WHERE fixture_id = %s
              AND type = 'Var'
              AND detail ILIKE 'Goal Disallowed%%'
            """,
            (match_id,),
        )
        max_id = int(mx["max_id"] or 0) if mx else 0

        execute(
            """
            UPDATE match_notification_state
            SET last_goal_disallowed_event_id = %s,
                updated_at = NOW()
            WHERE match_id = %s
            """,
            (max_id, match_id),
        )

        # 첫 루프는 알림 없이 종료 (과거 이벤트 폭탄 방지)
        return

    # ✅ goal disallowed가 새로 들어온 poll이고, raw 스코어가 감소한 경우에만 감소 허용
    allow_goal_decrease = False
    try:
        st0 = fetch_one(
            """
            SELECT last_goal_disallowed_event_id
            FROM match_notification_state
            WHERE match_id = %s
            """,
            (match_id,),
        )
        last_dis_id0 = int(st0["last_goal_disallowed_event_id"] or 0) if st0 else 0

        raw_decreased = False
        if last is not None:
            raw_decreased = (
                (current_raw.home_goals < last.home_goals) or
                (current_raw.away_goals < last.away_goals)
            )

        has_new_dis = False
        if raw_decreased:
            chk = fetch_one(
                """
                SELECT 1 AS ok
                FROM match_events
                WHERE fixture_id = %s
                  AND type = 'Var'
                  AND detail ILIKE 'Goal Disallowed%%'
                  AND id > %s
                LIMIT 1
                """,
                (match_id, last_dis_id0),
            )
            has_new_dis = bool(chk)

        allow_goal_decrease = raw_decreased and has_new_dis
    except Exception:
        log.exception("Failed to compute allow_goal_decrease for match %s", match_id)

    # ✅ 단조 상태 강제(필요 시 골 감소 허용) — 여기 1번만!
    current = apply_monotonic_state(last, current_raw, allow_goal_decrease=allow_goal_decrease)


    # ✅ state row가 없으면 먼저 생성 + VAR 포인터 초기화(과거 이벤트 폭탄 방지)
    state_exists = fetch_one(
        """
        SELECT 1 AS ok
        FROM match_notification_state
        WHERE match_id = %s
        """,
        (match_id,),
    )
    if not state_exists:
        # row 생성 (기본값 컬럼들도 함께 생김)
        save_state(current)

        # Goal Disallowed 포인터를 "현재까지 들어온 마지막 이벤트"로 올려서
        # 다음 루프에서 과거 VAR 이벤트가 대량 발송되지 않게 한다.
        mx = fetch_one(
            """
            SELECT COALESCE(MAX(id), 0) AS max_id
            FROM match_events
            WHERE fixture_id = %s
              AND type = 'Var'
              AND detail ILIKE 'Goal Disallowed%%'
            """,
            (match_id,),
        )
        max_id = int(mx["max_id"] or 0) if mx else 0

        execute(
            """
            UPDATE match_notification_state
            SET last_goal_disallowed_event_id = %s,
                updated_at = NOW()
            WHERE match_id = %s
            """,
            (max_id, match_id),
        )

    # 🔹 킥오프 10분 전 알림 시도 (status 가 NS/TBD 인 경우에만 내부에서 처리)
    try:
        maybe_send_kickoff_10m(fcm, current)
    except Exception:
        log.exception("Error while processing kickoff_10m for match %s", match_id)

    events = diff_events(last, current)


    # 팀/리그 이름 라벨을 한 번만 로딩해서 여러 이벤트에 사용
    labels = load_match_labels(match_id)

    # ==========================
    # ✅ VAR: Goal Disallowed 처리
    #  - match_notification_state.last_goal_disallowed_event_id 기준으로
    #    새로 들어온 Var 이벤트만 알림
    # ==========================
    try:
        st = fetch_one(
            """
            SELECT last_goal_disallowed_event_id
            FROM match_notification_state
            WHERE match_id = %s
            """,
            (match_id,),
        )
        # state row가 아직 없으면(첫 루프) 과거 이벤트를 쏘지 않기 위해 스킵
        if st:
            last_dis_id = int(st["last_goal_disallowed_event_id"] or 0)
            new_dis = load_new_goal_disallowed_events(match_id, last_dis_id)

            if new_dis:
                home_id = labels.get("home_id")
                away_id = labels.get("away_id")
                home_name = labels.get("home_name", "Home")
                away_name = labels.get("away_name", "Away")

                for ev in new_dis:
                    ev_id = int(ev["id"])
                    minute = int(ev.get("minute", 0) or 0)
                    extra_min = int(ev.get("extra", 0) or 0)
                    detail = str(ev.get("detail") or "")
                    team_id = ev.get("team_id")

                    if extra_min:
                        minute_str = f"{minute}+{extra_min}'"
                    else:
                        minute_str = f"{minute}'"

                    # 사유 추출: "Goal Disallowed - offside" -> "Offside"
                    reason = None
                    if " - " in detail:
                        reason_raw = detail.split(" - ", 1)[1].strip()
                        if reason_raw:
                            reason = reason_raw[:1].upper() + reason_raw[1:]

                    # 어느 팀 이벤트인지 판별
                    if team_id is not None and home_id is not None and int(team_id) == int(home_id):
                        dis_team = home_name
                    elif team_id is not None and away_id is not None and int(team_id) == int(away_id):
                        dis_team = away_name
                    else:
                        dis_team = None

                    extra_payload = {
                        "event_id": ev_id,
                        "disallowed_minute_str": minute_str,
                        "disallowed_team": dis_team,
                        "disallowed_reason": reason,
                        "disallowed_detail": detail,
                    }

                    tokens = get_tokens_for_event(match_id, "goal_disallowed")
                    if not tokens:
                        # 보낼 대상이 없으면 "처리 포인터"는 올려서
                        # 이후 구독자가 생겼을 때 과거 VAR 이벤트를 재전송하지 않도록 한다.
                        execute(
                            """
                            UPDATE match_notification_state
                            SET last_goal_disallowed_event_id = %s,
                                updated_at = NOW()
                            WHERE match_id = %s
                            """,
                            (ev_id, match_id),
                        )
                        last_dis_id = ev_id
                        continue


                    # 메시지 구성 (VAR는 "실제 스코어"를 보여줘야 함)
                    # current는 단조 상태라서 골 무효로 인한 스코어 감소가 반영되지 않을 수 있음
                    title, body = build_message("goal_disallowed", current_raw, extra_payload, labels)
                    data: Dict[str, Any] = {
                        "match_id": match_id,
                        "event_type": "goal_disallowed",
                    }
                    data.update(extra_payload)

                    batch_size = 500
                    send_ok = True
                    for i in range(0, len(tokens), batch_size):
                        batch = tokens[i : i + batch_size]
                        try:
                            resp = fcm.send_to_tokens(batch, title, body, data)
                            log.info(
                                "Sent goal_disallowed notification for match %s to %s devices: %s",
                                match_id,
                                len(batch),
                                resp,
                            )
                        except Exception:
                            send_ok = False
                            log.exception(
                                "Failed to send goal_disallowed notification for match %s (event_id=%s)",
                                match_id,
                                ev_id,
                            )
                            break


                    # ✅ 전송이 예외 없이 끝난 경우에만 last id 갱신 (누락 방지)
                    if send_ok:
                        execute(
                            """
                            UPDATE match_notification_state
                            SET last_goal_disallowed_event_id = %s,
                                updated_at = NOW()
                            WHERE match_id = %s
                            """,
                            (ev_id, match_id),
                        )
                        last_dis_id = ev_id
                    else:
                        # 실패 시 다음 루프에서 재시도
                        break
    except Exception:
        log.exception("Error while processing goal_disallowed for match %s", match_id)

    if not events:
        # 변화 없음 → 상태만 저장
        save_state(current)
        return

    for event_type, extra in events:
        # extra(튜플에서 온 dict-like)를 방어적으로 복사
        extra = dict(extra)


        # --- 이벤트 중복 방지를 위한 플래그 체크 로직 ---
        state_row = fetch_one(
            """
            SELECT
                kickoff_sent,
                kickoff_10m_sent,
                halftime_sent,
                secondhalf_sent,
                fulltime_sent,
                extra_time_start_sent,
                extra_time_halftime_sent,
                extra_time_secondhalf_sent,
                extra_time_end_sent,
                penalties_start_sent,
                penalties_end_sent
            FROM match_notification_state
            WHERE match_id = %s
            """,
            (match_id,),
        )


        # match_notification_state 에 row 가 없을 일은 거의 없지만,
        # 방어적으로 기본값 dict 하나 만들어둔다.
        if not state_row:
            state_row = {
                "kickoff_sent": False,
                "kickoff_10m_sent": False,
                "halftime_sent": False,
                "secondhalf_sent": False,
                "fulltime_sent": False,
                "extra_time_start_sent": False,
                "extra_time_halftime_sent": False,
                "extra_time_secondhalf_sent": False,
                "extra_time_end_sent": False,
                "penalties_start_sent": False,
                "penalties_end_sent": False,
            }

        flag_updates: List[str] = []

        # Kickoff
        if event_type == "kickoff":
            if state_row["kickoff_sent"]:
                # 이미 킥오프 알림 보냈으면 이번 이벤트는 skip
                continue
            flag_updates.append("kickoff_sent = TRUE")

        # Half-time (HT)
        if event_type == "ht":
            if state_row["halftime_sent"]:
                continue
            flag_updates.append("halftime_sent = TRUE")

        # Second half (2H)
        if event_type == "2h_start":
            if state_row["secondhalf_sent"]:
                continue
            flag_updates.append("secondhalf_sent = TRUE")

        # Full-time (FT)
        if event_type == "ft":
            if state_row["fulltime_sent"]:
                continue
            flag_updates.append("fulltime_sent = TRUE")

        # Extra time start
        if event_type == "et_start":
            if state_row["extra_time_start_sent"]:
                continue
            flag_updates.append("extra_time_start_sent = TRUE")

        # Extra time end
        if event_type == "et_end":
            if state_row["extra_time_end_sent"]:
                continue
            flag_updates.append("extra_time_end_sent = TRUE")

        # Penalties start
        if event_type == "pen_start":
            if state_row["penalties_start_sent"]:
                continue
            flag_updates.append("penalties_start_sent = TRUE")

        # Penalties end
        if event_type == "pen_end":
            if state_row["penalties_end_sent"]:
                continue
            flag_updates.append("penalties_end_sent = TRUE")

        # 플래그 DB 적용 (row 가 있을 때만 실제로 업데이트가 일어남)
        if flag_updates:
            execute(
                f"""
                UPDATE match_notification_state
                SET {", ".join(flag_updates)}
                WHERE match_id = %s
                """,
                (match_id,),
            )

 

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

        # redcard 이벤트라면, 마지막 레드카드 시간(분+추가시간)을 extra 에 추가
        if event_type == "redcard":
            red_time = load_last_redcard_minute(match_id)
            if red_time:
                minute = red_time.get("minute", 0)
                extra_min = red_time.get("extra", 0) or 0

                if extra_min:
                    # 예: 45+2'
                    red_minute_str = f"{minute}+{extra_min}'"
                else:
                    # 예: 78'
                    red_minute_str = f"{minute}'"

                extra["red_minute_str"] = red_minute_str


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
            seconds = 5  # 잘못된 값이면 기본 10초
        run_forever(seconds)
    else:
        run_once()

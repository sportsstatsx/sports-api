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

def _normalize_fixture_status(status: str, status_group: str | None) -> str:
    """
    ✅ fixtures 기준으로만 status를 정규화
    - status_group이 FINISHED면 status가 애매해도 FT로 처리(종료 알림/차단 안정화)
    - status_group=INPLAY인데 status가 누락(NS/TBD/빈값)이면 LIVE로 보정
    - HT는 group/status 둘 중 하나라도 HT면 HT
    """
    st = (status or "").strip()
    sg = (status_group or "").strip().upper()

    if sg in ("FINISHED", "FT"):
        if st in ("FT", "AET"):
            return st
        return "FT"

    if st == "HT" or sg == "HT":
        return "HT"

    if sg == "INPLAY" and st in ("", "NS", "TBD"):
        return "LIVE"

    return st


def load_match_elapsed(match_id: int) -> int | None:
    """
    fixtures에서 내려오는 elapsed(= matches.elapsed)를 사용.
    (득점/레드카드 알림에 분 표기용으로만 사용)
    """
    row = fetch_one(
        """
        SELECT elapsed
        FROM matches
        WHERE fixture_id = %s
        """,
        (match_id,),
    )
    if not row or row.get("elapsed") is None:
        return None
    try:
        return int(row["elapsed"])
    except Exception:
        return None

def _format_minute_with_extra(minute: Any, extra: Any) -> str | None:
    try:
        if minute is None:
            return None

        m = int(minute)

        if extra is None:
            return f"{m}'"

        e = int(extra)
        if e > 0:
            return f"{m}+{e}'"

        return f"{m}'"
    except Exception:
        return None


def _detect_scoring_team_id(
    old_home: int,
    old_away: int,
    new_home: int,
    new_away: int,
    home_id: int | None,
    away_id: int | None,
) -> int | None:
    if (new_home > old_home) and (new_away == old_away):
        return home_id
    if (new_away > old_away) and (new_home == old_home):
        return away_id
    return None


def _detect_red_team_id(
    old_home_red: int,
    old_away_red: int,
    new_home_red: int,
    new_away_red: int,
    home_id: int | None,
    away_id: int | None,
) -> int | None:
    if (new_home_red > old_home_red) and (new_away_red == old_away_red):
        return home_id
    if (new_away_red > old_away_red) and (new_home_red == old_home_red):
        return away_id
    return None


def load_goal_minute_str_for_score(
    match_id: int,
    team_id: int | None,
    expected_goal_number: int,
) -> str | None:
    """
    현재 반영된 스코어 기준으로 "그 팀의 n번째 골" minute 문자열을 찾는다.

    예:
    - 홈이 2번째 골을 넣어 2:1이 됐다면 -> 홈팀의 2번째 Goal 이벤트를 찾음
    - 원정이 3번째 골을 넣어 1:3이 됐다면 -> 원정팀의 3번째 Goal 이벤트를 찾음

    중요:
    - 단순히 최신 Goal 1개를 읽으면, score는 이미 올라갔지만 match_events 적재가 늦은 순간
      예전 골 minute(예: 6')를 다시 보내는 버그가 생긴다.
    - 따라서 expected_goal_number 번째 Goal 이벤트가 실제로 존재할 때만 그 minute를 사용하고,
      아직 없으면 None 을 반환해서 elapsed fallback 으로 넘긴다.
    """
    try:
        expected_n = int(expected_goal_number)
    except Exception:
        return None

    if expected_n <= 0:
        return None

    count_row = fetch_one(
        """
        SELECT COUNT(*) AS cnt
        FROM match_events
        WHERE fixture_id = %s
          AND LOWER(COALESCE(type, '')) = 'goal'
          AND (%s IS NULL OR team_id = %s)
        """,
        (match_id, team_id, team_id),
    )

    try:
        current_count = int(count_row.get("cnt") or 0) if count_row else 0
    except Exception:
        current_count = 0

    # 아직 그 팀의 n번째 골 이벤트가 DB에 안 들어온 상태
    # -> stale minute 사용 금지, elapsed fallback 하도록 None 반환
    if current_count < expected_n:
        return None

    row = fetch_one(
        """
        SELECT
            minute,
            extra
        FROM match_events
        WHERE fixture_id = %s
          AND LOWER(COALESCE(type, '')) = 'goal'
          AND (%s IS NULL OR team_id = %s)
        ORDER BY id ASC
        OFFSET %s
        LIMIT 1
        """,
        (match_id, team_id, team_id, expected_n - 1),
    )
    if not row:
        return None

    return _format_minute_with_extra(row.get("minute"), row.get("extra"))


def load_latest_red_minute_str(match_id: int, team_id: int | None = None) -> str | None:
    """
    방금 반영된 레드카드 알림용 minute 문자열을 match_events에서 찾는다.
    - detail 이 'Red Card' 이거나 VAR 'Card upgrade' 인 이벤트를 레드 후보로 본다.
    - 추가시간은 minute + extra 조합으로 만든다. (예: 90+1')
    """
    row = fetch_one(
        """
        SELECT
            minute,
            extra
        FROM match_events
        WHERE fixture_id = %s
          AND (%s IS NULL OR team_id = %s)
          AND (
                POSITION('red card' IN LOWER(COALESCE(detail, ''))) > 0
                OR POSITION('card upgrade' IN LOWER(COALESCE(detail, ''))) > 0
              )
        ORDER BY id DESC
        LIMIT 1
        """,
        (match_id, team_id, team_id),
    )
    if not row:
        return None
    return _format_minute_with_extra(row.get("minute"), row.get("extra"))



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
    ✅ fixtures 기준으로만 현재 상태를 읽는다.

    - 스코어: matches.home_ft / away_ft (=/fixtures 기반)
    - status: matches.status (+ matches.status_group 보정)
    - 레드카드: match_live_state.home_red / away_red "만" 사용 (없으면 0)
      -> match_events는 레드카드 판단에 사용하지 않는다.
    """
    base = fetch_one(
        """
        SELECT
            m.fixture_id AS match_id,
            m.status     AS status,
            m.status_group AS status_group,
            m.home_id    AS home_id,
            m.away_id    AS away_id,
            COALESCE(m.home_ft, 0) AS home_goals,
            COALESCE(m.away_ft, 0) AS away_goals
        FROM matches m
        WHERE m.fixture_id = %s
        """,
        (match_id,),
    )

    if not base:
        return None

    # fixtures 기반 status 정규화
    eff_status = _normalize_fixture_status(
        str(base["status"]) if base.get("status") is not None else "",
        str(base["status_group"]) if base.get("status_group") is not None else "",
    )

    # ✅ 레드카드는 match_live_state만
    home_red = 0
    away_red = 0
    try:
        r = fetch_one(
            """
            SELECT
                COALESCE(home_red, 0) AS home_red,
                COALESCE(away_red, 0) AS away_red
            FROM match_live_state
            WHERE fixture_id = %s
            """,
            (match_id,),
        )
        if r:
            home_red = int(r.get("home_red") or 0)
            away_red = int(r.get("away_red") or 0)
    except Exception:
        # 테이블이 없거나 조회 실패해도 "0"으로만 간다(요구사항: match_live_state만 본다)
        home_red = 0
        away_red = 0

    return MatchState(
        match_id=int(base["match_id"]),
        status=eff_status,
        home_goals=int(base.get("home_goals") or 0),
        away_goals=int(base.get("away_goals") or 0),
        home_red=int(home_red),
        away_red=int(away_red),
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

    - 알림 메시지(타이틀/바디) 생성에 필요한 팀명/리그명을 제공한다.
    - Goal Disallowed 관련 알림은 제거되었으므로, 해당 용도로는 사용하지 않는다.
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



def apply_monotonic_state(
    last: MatchState | None,
    current: MatchState,
) -> MatchState:

    if last is None:
        return current

    old_status = last.status or ""
    new_status = current.status or ""

    old_rank = STATUS_ORDER.get(old_status, 0)
    new_rank = STATUS_ORDER.get(new_status, 0)

    # status만 단조 보정
    if new_rank < old_rank:
        effective_status = old_status
    else:
        effective_status = new_status

    # ✅ goals는 max 금지 (가짜 스코어 합성의 근본 원인)
    # event 기반 스코어는 VAR로 감소할 수 있고, 그게 정상 동작이다.
    return MatchState(
        match_id=current.match_id,
        status=effective_status,
        home_goals=current.home_goals,
        away_goals=current.away_goals,
        home_red=max(last.home_red, current.home_red),
        away_red=max(last.away_red, current.away_red),
    )




def diff_events(old: MatchState | None, new: MatchState) -> List[Tuple[str, Dict[str, Any]]]:
    events: List[Tuple[str, Dict[str, Any]]] = []

    if old is None:
        return events

    old_status = old.status or ""
    new_status = new.status or ""

    if old_status in ("FT", "AET"):
        return events

    # 1) Kickoff (fixtures status 전환 기반)
    if old_status in ("", "NS", "TBD") and new_status not in ("", "NS", "TBD"):
        events.append(("kickoff", {}))

    # 2) Half-time
    if new_status == "HT" and old_status != "HT":
        events.append(("ht", {}))

    # 3) Second half start
    if old_status == "HT" and new_status in ("2H", "LIVE"):
        events.append(("2h_start", {}))

    # 4) ET/PEN/FT 흐름
    if old_status not in ("ET", "AET", "P", "PEN") and new_status == "ET":
        events.append(("et_start", {}))

    if old_status == "ET" and new_status in ("AET", "FT"):
        events.append(("et_end", {}))
        events.append(("ft", {}))

    if old_status == "ET" and new_status in ("P", "PEN"):
        events.append(("et_end", {}))
        events.append(("pen_start", {}))

    if old_status in ("P", "PEN") and new_status in ("FT", "AET"):
        events.append(("pen_end", {}))
        events.append(("ft", {}))

    ft_transition = (old_status not in ("FT", "AET")) and (new_status in ("FT", "AET"))
    if ft_transition:
        if not any(ev[0] == "ft" for ev in events):
            events.append(("ft", {}))

    # ✅ score: fixtures 스코어 변화로 감지
    if (new.home_goals != old.home_goals) or (new.away_goals != old.away_goals):
        payload = {"old_home": old.home_goals, "old_away": old.away_goals}

        # 감소/정정은 별도 이벤트로(선택 알림)
        if (new.home_goals < old.home_goals) or (new.away_goals < old.away_goals):
            events.append(("score_correction", payload))
        else:
            events.append(("score", payload))

    # ✅ Red card: match_live_state 값 증가만 감지
    if new.home_red > old.home_red or new.away_red > old.away_red:
        events.append(("redcard", {"old_home": old.home_red, "old_away": old.away_red}))

    return events







def get_tokens_for_event(match_id: int, event_type: str) -> List[str]:
    """
    이벤트 종류에 따라 해당 옵션을 켜둔 구독자 토큰만 가져오기.

    ✅ 개선:
    - fcm_token NULL/빈값/공백 제거 (FCM 예외로 인한 무한 재전송/반복 스팸 방지에 핵심)
    - DISTINCT 로 중복 토큰 제거
    - score 정정 알림(score_correction)도 notify_score 옵션에 묶음
    """
    option_column = {
        # 킥오프 관련
        "kickoff_10m": "notify_kickoff",  # 🔹 킥오프 10분 전
        "kickoff": "notify_kickoff",

        # 득점 / 카드
        "score": "notify_score",
        "score_correction": "notify_score",  # ✅ 스코어 정정 알림(선택 기능)
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
        SELECT DISTINCT u.fcm_token
        FROM match_notification_subscriptions s
        JOIN user_devices u ON u.device_id = s.device_id
        WHERE s.match_id = %s
          AND s.{option_column} = TRUE
          AND u.notifications_enabled = TRUE
          AND u.fcm_token IS NOT NULL
          AND BTRIM(u.fcm_token) <> ''
          AND LOWER(BTRIM(u.fcm_token)) <> 'none'
        """,
        (match_id,),
    )

    # 방어적으로 strip + 빈값 제거
    out: List[str] = []
    for r in rows:
        tok = r.get("fcm_token")
        if tok is None:
            continue
        s = str(tok).strip()
        if not s:
            continue
        if s.lower() == "none":
            continue
        out.append(s)
    return out





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
    - ✅ score_correction(스코어 정정) 알림 지원
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

    # ✅ Score correction
    if event_type == "score_correction":
        old_home = extra.get("old_home")
        old_away = extra.get("old_away")
        if old_home is not None and old_away is not None:
            title = f"🔄 Score corrected ({int(old_home)}–{int(old_away)} → {match.home_goals}–{match.away_goals})"
        else:
            title = "🔄 Score corrected"
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

    ✅ 개선(기존 동작 유지 + 버그 수정):
    - 전송이 전부 실패했는데도 kickoff_10m_sent=TRUE 찍혀서 영구 누락되는 케이스 방지
      -> "한 배치라도 성공"했을 때만 플래그 ON
    """
    if match.status not in ("", "NS", "TBD"):
        return

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
        kickoff_dt = datetime.fromisoformat(str(row["date_utc"]))
        # ✅ tz 없는 datetime(naive)로 들어오는 경우 방지
        if kickoff_dt.tzinfo is None:
            kickoff_dt = kickoff_dt.replace(tzinfo=timezone.utc)
    except Exception:
        return

    now_utc = datetime.now(timezone.utc)
    try:
        diff_sec = (kickoff_dt - now_utc).total_seconds()
    except Exception:
        # ✅ 혹시라도 tz 혼종이면 안전하게 스킵
        return

    if not (0 <= diff_sec <= 600):
        return


    state_row = fetch_one(
        """
        SELECT kickoff_10m_sent
        FROM match_notification_state
        WHERE match_id = %s
        """,
        (match.match_id,),
    )
    if not state_row:
        return
    if state_row["kickoff_10m_sent"]:
        return

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

    batch_size = 500
    any_success = False

    for i in range(0, len(tokens), batch_size):
        batch = tokens[i : i + batch_size]
        try:
            resp = fcm.send_to_tokens(batch, title, body, data)
            any_success = True
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

    if any_success:
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
    # ✅ fixtures 기반(=matches에서 읽음) + red는 match_live_state만
    current_raw = load_current_match_state(match_id)
    if not current_raw:
        log.info("match_id=%s current state not found, skip", match_id)
        return

    # ✅ 종료(FT/AET)도 "종료 이벤트(ft/et_end/pen_end)"는 1회 발송 기회가 있어야 한다.
    # 다만 워커가 오래 멈췄다 재개된 경우 kickoff/ht/2h 같은 "과거 단계 알림 폭탄"은 막는다.
    if (current_raw.status or "") in ("FT", "AET"):
        last = load_last_state(match_id)

        # state row 없으면: 현재값으로 초기화 + 플래그 잠금만 하고 종료(늦은 구독/늦은 부팅 폭탄 방지)
        state_exists = fetch_one(
            """
            SELECT 1 AS ok
            FROM match_notification_state
            WHERE match_id = %s
            """,
            (match_id,),
        )
        if not state_exists:
            save_state(current_raw)

            execute(
                """
                UPDATE match_notification_state
                SET
                  kickoff_sent = TRUE,
                  kickoff_10m_sent = TRUE,
                  halftime_sent = TRUE,
                  secondhalf_sent = TRUE,
                  fulltime_sent = TRUE,
                  extra_time_start_sent = TRUE,
                  extra_time_halftime_sent = TRUE,
                  extra_time_secondhalf_sent = TRUE,
                  extra_time_end_sent = TRUE,
                  penalties_start_sent = TRUE,
                  penalties_end_sent = TRUE,

                  updated_at = NOW()
                WHERE match_id = %s
                """,
                (match_id,),
            )
            return

        # ✅ status/red 단조 보정(점수는 그대로)
        current = apply_monotonic_state(last, current_raw)

        # ✅ 종료 시점 이벤트만 추려서 발송(과거 단계 이벤트는 버림)
        finish_events = [ev for ev in diff_events(last, current) if ev[0] in ("et_end", "pen_end", "ft")]

        labels = load_match_labels(match_id)

        flag_column_by_event: Dict[str, str] = {
            "ft": "fulltime_sent",
            "et_end": "extra_time_end_sent",
            "pen_end": "penalties_end_sent",
        }

        for event_type, extra in finish_events:
            extra = dict(extra)

            flag_col = flag_column_by_event.get(event_type)
            flag_was_set = False
            if flag_col:
                got = fetch_one(
                    f"""
                    UPDATE match_notification_state
                    SET {flag_col} = TRUE
                    WHERE match_id = %s
                      AND {flag_col} = FALSE
                    RETURNING 1 AS ok
                    """,
                    (match_id,),
                )
                if not got:
                    continue
                flag_was_set = True

            tokens = get_tokens_for_event(match_id, event_type)
            if not tokens:
                continue

            title, body = build_message(event_type, current, extra, labels)
            data: Dict[str, Any] = {"match_id": match_id, "event_type": event_type}
            data.update(extra)

            batch_size = 500
            send_failed = False
            for i in range(0, len(tokens), batch_size):
                batch = tokens[i : i + batch_size]
                try:
                    resp = fcm.send_to_tokens(batch, title, body, data)
                    log.info("Sent %s notification for match %s to %s devices: %s", event_type, match_id, len(batch), resp)
                except Exception:
                    send_failed = True
                    log.exception("Failed to send %s notification for match %s", event_type, match_id)
                    break

            if send_failed and flag_was_set and flag_col:
                try:
                    execute(
                        f"""
                        UPDATE match_notification_state
                        SET {flag_col} = FALSE
                        WHERE match_id = %s
                        """,
                        (match_id,),
                    )
                except Exception:
                    log.exception("Failed to rollback flag %s for match %s after send failure", flag_col, match_id)

        # ✅ 마지막으로 “잠금”(기존 의도 유지)
        save_state(current)

        # ✅ 종료 이후 단계 플래그 잠금(기존 의도 유지)
        execute(
            """
            UPDATE match_notification_state
            SET
              kickoff_sent = TRUE,
              kickoff_10m_sent = TRUE,
              halftime_sent = TRUE,
              secondhalf_sent = TRUE,
              fulltime_sent = TRUE,
              extra_time_start_sent = TRUE,
              extra_time_halftime_sent = TRUE,
              extra_time_secondhalf_sent = TRUE,
              extra_time_end_sent = TRUE,
              penalties_start_sent = TRUE,
              penalties_end_sent = TRUE,

              updated_at = NOW()
            WHERE match_id = %s
            """,
            (match_id,),
        )
        return

    last = load_last_state(match_id)

    state_exists = fetch_one(
        """
        SELECT 1 AS ok
        FROM match_notification_state
        WHERE match_id = %s
        """,
        (match_id,),
    )

    # ✅ state row 없으면: 현재값으로만 초기화하고 알림은 보내지 않음(폭탄 방지)
    if not state_exists:
        save_state(current_raw)

        try:
            maybe_send_kickoff_10m(fcm, current_raw)
        except Exception:
            log.exception("Error while processing kickoff_10m on first state init for match %s", match_id)

        return

    # ✅ status/red는 단조 보정, score는 fixtures 값 그대로(감소는 score_correction으로 감지)
    current = apply_monotonic_state(last, current_raw)

    try:
        maybe_send_kickoff_10m(fcm, current)
    except Exception:
        log.exception("Error while processing kickoff_10m for match %s", match_id)

    labels = load_match_labels(match_id)

    # elapsed(분 표기) - fixtures 기반
    elapsed = load_match_elapsed(match_id)

    # ==========================
    # ✅ fixtures 기반 score/status/red 변화(diff_events)로만 알림 생성
    # ==========================
    events = diff_events(last, current)

    if not events:
        save_state(current)
        return

    flag_column_by_event: Dict[str, str] = {
        "kickoff": "kickoff_sent",
        "ht": "halftime_sent",
        "2h_start": "secondhalf_sent",
        "ft": "fulltime_sent",
        "et_start": "extra_time_start_sent",
        "et_end": "extra_time_end_sent",
        "pen_start": "penalties_start_sent",
        "pen_end": "penalties_end_sent",
    }

    for event_type, extra in events:
        extra = dict(extra)

        # ✅ score/redcard는 match_events.minute + extra 기준으로 분 표시
        if event_type == "score":
            old_home = int(extra.get("old_home", current.home_goals))
            old_away = int(extra.get("old_away", current.away_goals))

            home_id = labels.get("home_id")
            away_id = labels.get("away_id")

            scoring_team_id = _detect_scoring_team_id(
                old_home=old_home,
                old_away=old_away,
                new_home=current.home_goals,
                new_away=current.away_goals,
                home_id=home_id,
                away_id=away_id,
            )

            expected_goal_number: int | None = None
            if scoring_team_id is not None:
                if home_id is not None and scoring_team_id == home_id:
                    expected_goal_number = int(current.home_goals)
                elif away_id is not None and scoring_team_id == away_id:
                    expected_goal_number = int(current.away_goals)

            goal_minute_str = None
            if expected_goal_number is not None and expected_goal_number > 0:
                goal_minute_str = load_goal_minute_str_for_score(
                    match_id=match_id,
                    team_id=scoring_team_id,
                    expected_goal_number=expected_goal_number,
                )

            if goal_minute_str:
                extra["goal_minute_str"] = goal_minute_str
            elif elapsed is not None and elapsed > 0:
                # match_events 적재가 아직 안 따라온 경우 stale minute 대신 현재 elapsed 사용
                extra["goal_minute_str"] = f"{int(elapsed)}'"

        if event_type == "redcard":
            old_home_red = int(extra.get("old_home", current.home_red))
            old_away_red = int(extra.get("old_away", current.away_red))

            red_team_id = _detect_red_team_id(
                old_home_red=old_home_red,
                old_away_red=old_away_red,
                new_home_red=current.home_red,
                new_away_red=current.away_red,
                home_id=labels.get("home_id"),
                away_id=labels.get("away_id"),
            )

            red_minute_str = load_latest_red_minute_str(match_id, red_team_id)

            if red_minute_str:
                extra["red_minute_str"] = red_minute_str
            elif elapsed is not None and elapsed > 0:
                extra["red_minute_str"] = f"{int(elapsed)}'"

        # ✅ 단계성 이벤트만 플래그 잠금(스코어/정정/레드는 플래그 없음)
        flag_col = flag_column_by_event.get(event_type)
        flag_was_set = False
        if flag_col:
            got = fetch_one(
                f"""
                UPDATE match_notification_state
                SET {flag_col} = TRUE
                WHERE match_id = %s
                  AND {flag_col} = FALSE
                RETURNING 1 AS ok
                """,
                (match_id,),
            )
            if not got:
                continue
            flag_was_set = True

        tokens = get_tokens_for_event(match_id, event_type)
        if not tokens:
            continue

        title, body = build_message(event_type, current, extra, labels)
        data: Dict[str, Any] = {"match_id": match_id, "event_type": event_type}
        data.update(extra)

        batch_size = 500
        send_failed = False
        for i in range(0, len(tokens), batch_size):
            batch = tokens[i : i + batch_size]
            try:
                resp = fcm.send_to_tokens(batch, title, body, data)
                log.info("Sent %s notification for match %s to %s devices: %s", event_type, match_id, len(batch), resp)
            except Exception:
                send_failed = True
                log.exception("Failed to send %s notification for match %s", event_type, match_id)
                break

        if send_failed and flag_was_set and flag_col:
            try:
                execute(
                    f"""
                    UPDATE match_notification_state
                    SET {flag_col} = FALSE
                    WHERE match_id = %s
                    """,
                    (match_id,),
                )
            except Exception:
                log.exception("Failed to rollback flag %s for match %s after send failure", flag_col, match_id)

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

    ✅ 워커 재시작(재배포) 직후 1회 "부트스트랩":
    - match_notification_state를 현재 시점으로 동기화하되 알림은 보내지 않는다.
      -> 재배포 순간의 kickoff/ht/2h/ft 등 "알림 폭탄" 방지.
    - score/score_correction 감지는 match_notification_state의
      last_home_goals/last_away_goals(=save_state)로만 충분하므로,
      last_goal_event_id/last_goal_home_goals/last_goal_away_goals 는 더 이상 사용하지 않는다.
    - goal_disallowed 기능 제거에 맞춰 last_goal_disallowed_event_id 포인터도 0으로 정리한다.
    """
    fcm = FCMClient()
    log.info(
        "Starting match_event_worker in worker mode (interval=%s sec)",
        interval_seconds,
    )

    # --------------------------
    # ✅ BOOTSTRAP (재시작 1회)
    # --------------------------
    try:
        matches = get_subscribed_matches()
        if matches:
            log.info(
                "Bootstrap: syncing notification state for %s subscribed matches (no notifications).",
                len(matches),
            )

        for match_id in matches:
            current_raw = load_current_match_state(match_id)
            if not current_raw:
                continue

            # state row 보장 + last_status/last_goals/last_red = 현재로 맞춤
            # (score/score_correction 감지는 이 값들로만 충분)
            save_state(current_raw)

            # 단계 플래그를 "현재 상태 기준"으로 잠가서
            # 재시작 직후 kickoff/ht/2h/ft/et/pen 단계 알림이 튀지 않게
            st = (current_raw.status or "").strip()
            rank = STATUS_ORDER.get(st, 0)

            kickoff_sent = (st not in ("", "NS", "TBD")) and (rank >= 10 or st == "LIVE")
            halftime_sent = rank >= 20
            secondhalf_sent = rank >= 30
            extra_time_start_sent = rank >= 40
            extra_time_end_sent = rank >= 60  # AET(60) 이상이면 ET 종료는 이미 지난 상태
            penalties_start_sent = rank >= 50  # P(50) / PEN(70)
            penalties_end_sent = rank >= 80     # FT/AET면 승부차기도 이미 끝났다고 간주(FT에서만 true 의미)
            fulltime_sent = rank >= 80

            # ✅ kickoff_10m도 재배포 직후 "잠금" 처리
            # - 재배포 시점이 킥오프 10분 전 구간에 걸리면 원치 않는 알림이 튈 수 있음
            kickoff_10m_sent = kickoff_sent  # 킥오프가 이미 진행/진입이면 10분전은 의미 없음

            execute(
                """
                UPDATE match_notification_state
                SET
                  last_goal_disallowed_event_id = 0,

                  kickoff_10m_sent = %s,
                  kickoff_sent = %s,
                  halftime_sent = %s,
                  secondhalf_sent = %s,
                  extra_time_start_sent = %s,
                  extra_time_end_sent = %s,
                  penalties_start_sent = %s,
                  penalties_end_sent = %s,
                  fulltime_sent = %s,

                  updated_at = NOW()
                WHERE match_id = %s
                """,
                (
                    bool(kickoff_10m_sent),
                    bool(kickoff_sent),
                    bool(halftime_sent),
                    bool(secondhalf_sent),
                    bool(extra_time_start_sent),
                    bool(extra_time_end_sent),
                    bool(penalties_start_sent),
                    bool(penalties_end_sent),
                    bool(fulltime_sent),
                    match_id,
                ),
            )

    except Exception:
        log.exception("Bootstrap failed (will continue normal loop)")

    # --------------------------
    # NORMAL LOOP
    # --------------------------
    while True:
        try:
            run_once(fcm)
        except Exception:
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

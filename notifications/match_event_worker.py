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

def calc_score_from_db_events(
    rows: List[Dict[str, Any]],
    home_id: int,
    away_id: int,
    hint_home_ft: int,
    hint_away_ft: int,
) -> Tuple[int, int]:
    """
    DB의 match_events(Goal/Var)로부터 타임라인 규칙 기반 스코어를 계산.
    - Missed Penalty 제외
    - Var(Goal Disallowed/Cancelled/No Goal)로 직전 골 취소 처리(보수적)
    - Own Goal은 team_id를 반대로 뒤집어 1점 처리(타임라인과 동일한 의도)
    """
    def _norm(s: Any) -> str:
        if s is None:
            return ""
        x = str(s).lower().strip()
        x = " ".join(x.split())
        return x

    invalid_markers = ("cancel", "disallow", "no goal", "offside", "foul", "annul", "null")

    # goals: {team_id, is_og, minute, extra, cancelled}
    goals: List[Dict[str, Any]] = []

    # 이미 rows가 정렬되어 들어온다고 가정(혹시 몰라 한번 더)
    def _key(r: Dict[str, Any]) -> Tuple[int, int, int]:
        m = r.get("minute")
        e = r.get("extra")
        i = r.get("id")
        mm = int(m) if m is not None else 10**9
        ee = int(e) if e is not None else 0
        ii = int(i) if i is not None else 0
        return (mm, ee, ii)

    evs = sorted(rows or [], key=_key)

    def _add_goal(r: Dict[str, Any]) -> None:
        detail = _norm(r.get("detail"))

        # 실축PK 제외
        if "missed penalty" in detail:
            return
        if ("miss" in detail) and ("pen" in detail):
            return

        # Goal.detail에 취소/무효 문구가 붙는(드문) 케이스 방어(OG는 예외)
        if any(m in detail for m in invalid_markers) and ("own goal" not in detail):
            return

        tid = r.get("team_id")
        if tid is None:
            return
        team_id = int(tid)

        minute = int(r.get("minute") or 0) if r.get("minute") is not None else 0
        extra = int(r.get("extra") or 0)

        is_og = ("own goal" in detail)

        goals.append(
            {
                "team_id": team_id,
                "is_og": bool(is_og),
                "minute": minute,
                "extra": extra,
                "cancelled": False,
            }
        )

    def _apply_var(r: Dict[str, Any]) -> None:
        detail = _norm(r.get("detail"))
        if not detail:
            return

        is_disallow = ("goal disallowed" in detail) or ("goal cancelled" in detail) or ("no goal" in detail)
        if not is_disallow:
            return

        var_team_id = r.get("team_id")
        var_team_id = int(var_team_id) if var_team_id is not None else None
        var_minute = r.get("minute")
        if var_minute is None:
            return
        var_elapsed = int(var_minute)

        # 보수적 취소: 같은 분(우선) -> +-1 -> +-2 범위에서 직전 골 취소
        def _pick_cancel_idx(max_delta: int) -> int | None:
            best: int | None = None
            for i in range(len(goals) - 1, -1, -1):
                g = goals[i]
                if g.get("cancelled"):
                    continue
                g_el = g.get("minute")
                if g_el is None:
                    continue
                if abs(int(g_el) - var_elapsed) > max_delta:
                    continue

                if var_team_id is not None:
                    if int(g.get("team_id")) == var_team_id:
                        return i
                    if best is None:
                        best = i
                else:
                    return i
            return best

        idx = _pick_cancel_idx(0)
        if idx is None:
            idx = _pick_cancel_idx(1)
        if idx is None:
            idx = _pick_cancel_idx(2)

        if idx is not None:
            goals[idx]["cancelled"] = True

    for r in evs:
        t = _norm(r.get("type"))
        if t == "goal":
            _add_goal(r)
        elif t == "var":
            _apply_var(r)

    def _sum_scores() -> Tuple[int, int]:
        h = 0
        a = 0
        for g in goals:
            if g.get("cancelled"):
                continue
            tid = int(g.get("team_id"))
            is_og = bool(g.get("is_og"))

            scoring_tid = tid
            if is_og:
                if tid == home_id:
                    scoring_tid = away_id
                elif tid == away_id:
                    scoring_tid = home_id

            if scoring_tid == home_id:
                h += 1
            elif scoring_tid == away_id:
                a += 1
        return h, a

    h, a = _sum_scores()

    # hint는 "OG flip 방향이 섞이는 공급자 케이스"까지 완벽히 잡으려면 필요하지만,
    # 지금은 알림 worker에서 타임라인과 동일하게 OG를 반대로 처리하는 게 1차 목표라
    # hint는 참고용으로만 둔다(필요 시 여기서 분기 확장 가능).
    return h, a



def load_current_match_state(match_id: int) -> MatchState | None:
    """
    현재 match_id 경기의 상태를 DB에서 읽어서 MatchState로 반환한다.

    변경:
    - 골 수(home_goals/away_goals)는 matches.home_ft/away_ft를 직접 쓰지 않고,
      match_events(Goal + Var) 기반으로 "타임라인 규칙"으로 계산한다.
      -> 스냅샷 흔들림/롤백에도 알림 기준 스코어가 안정적

    - 레드카드 COUNT는 기존처럼 1회 JOIN+집계 유지
    """
    base = fetch_one(
        """
        SELECT
            m.fixture_id AS match_id,
            m.status     AS status,
            m.home_id    AS home_id,
            m.away_id    AS away_id,
            COALESCE(m.home_ft, 0) AS hint_home_ft,
            COALESCE(m.away_ft, 0) AS hint_away_ft,
            COALESCE(
                SUM(
                    CASE
                        WHEN e.type = 'Card'
                         AND e.detail IN ('Red Card', 'Second Yellow Card')
                         AND e.team_id = m.home_id
                        THEN 1 ELSE 0
                    END
                ),
                0
            ) AS home_red,
            COALESCE(
                SUM(
                    CASE
                        WHEN e.type = 'Card'
                         AND e.detail IN ('Red Card', 'Second Yellow Card')
                         AND e.team_id = m.away_id
                        THEN 1 ELSE 0
                    END
                ),
                0
            ) AS away_red
        FROM matches m
        LEFT JOIN match_events e
               ON e.fixture_id = m.fixture_id
              AND e.type = 'Card'
              AND e.detail IN ('Red Card', 'Second Yellow Card')
              AND e.team_id IN (m.home_id, m.away_id)
        WHERE m.fixture_id = %s
        GROUP BY
            m.fixture_id, m.status, m.home_id, m.away_id, m.home_ft, m.away_ft
        """,
        (match_id,),
    )

    if not base:
        return None

    home_id = int(base["home_id"]) if base["home_id"] is not None else 0
    away_id = int(base["away_id"]) if base["away_id"] is not None else 0

    # 타임라인 규칙 기반 골 계산(Goal + Var만)
    ev_rows = fetch_all(
        """
        SELECT
            id,
            type,
            detail,
            team_id,
            minute,
            COALESCE(extra, 0) AS extra
        FROM match_events
        WHERE fixture_id = %s
          AND type IN ('Goal', 'Var')
        ORDER BY
          minute ASC NULLS LAST,
          extra ASC NULLS LAST,
          id ASC
        """,
        (match_id,),
    )

    hint_h = int(base["hint_home_ft"] or 0)
    hint_a = int(base["hint_away_ft"] or 0)

    # 아래 calc_score_from_db_events()는 이 파일에 추가할 헬퍼(아래에 제공)
    hg, ag = calc_score_from_db_events(ev_rows, home_id, away_id, hint_h, hint_a)

    return MatchState(
        match_id=int(base["match_id"]),
        status=str(base["status"]) if base["status"] is not None else "",
        home_goals=int(hg),
        away_goals=int(ag),
        home_red=int(base["home_red"] or 0),
        away_red=int(base["away_red"] or 0),
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

    # 1) Kickoff
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

    # ✅ score는 여기서 감지하지 않는다 (match_events 포인터 기반으로 별도 처리)

    # 6) Red card (증가만 감지)
    if new.home_red > old.home_red or new.away_red > old.away_red:
        events.append(("redcard", {"old_home": old.home_red, "old_away": old.away_red}))

    return events






def get_tokens_for_event(match_id: int, event_type: str) -> List[str]:
    """
    이벤트 종류에 따라 해당 옵션을 켜둔 구독자 토큰만 가져오기.

    ✅ 개선:
    - fcm_token NULL/빈값/공백 제거 (FCM 예외로 인한 무한 재전송/반복 스팸 방지에 핵심)
    - DISTINCT 로 중복 토큰 제거
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
    except Exception:
        return

    now_utc = datetime.now(timezone.utc)
    diff_sec = (kickoff_dt - now_utc).total_seconds()
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
    current_raw = load_current_match_state(match_id)
    if not current_raw:
        log.info("match_id=%s current state not found, skip", match_id)
        return

    # ✅ (핵심) 종료된 경기면: 알림 로직을 아예 타지 않게 막고,
    # 포인터/플래그를 "현재 시점"으로 정리해서 재배포/재시작 때 폭탄을 방지한다.
    if (current_raw.status or "") in ("FT", "AET"):
        # state row 없으면 생성 (ON CONFLICT라 있어도 안전)
        save_state(current_raw)

        # 현재 DB 기준 MAX 포인터로 당겨서 "과거 골/VAR"가 new로 읽히는 걸 막음
        gx = fetch_one(
            """
            SELECT COALESCE(MAX(id), 0) AS max_id
            FROM match_events
            WHERE fixture_id = %s
              AND type = 'Goal'
            """,
            (match_id,),
        )
        max_goal_id = int(gx["max_id"] or 0) if gx else 0

        vx = fetch_one(
            """
            SELECT COALESCE(MAX(id), 0) AS max_id
            FROM match_events
            WHERE fixture_id = %s
              AND type = 'Var'
              AND detail ILIKE 'Goal Disallowed%%'
            """,
            (match_id,),
        )
        max_dis_id = int(vx["max_id"] or 0) if vx else 0

        # 종료된 경기는 단계성 알림도 더 이상 필요 없으니 전부 TRUE로 잠금
        # (컬럼은 네 테이블 스크린샷 기준)
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

              last_goal_event_id = %s,
              last_goal_disallowed_event_id = %s,
              last_goal_home_goals = %s,
              last_goal_away_goals = %s,

              updated_at = NOW()
            WHERE match_id = %s
            """,
            (
                max_goal_id,
                max_dis_id,
                int(current_raw.home_goals),
                int(current_raw.away_goals),
                match_id,
            ),
        )

        # ✅ 종료된 경기에서는 어떤 알림도 보내지 않음
        return

    last = load_last_state(match_id)

    # state row 존재 확인
    state_exists = fetch_one(
        """
        SELECT 1 AS ok
        FROM match_notification_state
        WHERE match_id = %s
        """,
        (match_id,),
    )

    # ✅ state row가 없으면 먼저 생성 + 포인터 초기화(과거 이벤트 폭탄 방지)
    if not state_exists:
        save_state(current_raw)

        # kickoff_10m 즉시 체크(기존 유지)
        try:
            maybe_send_kickoff_10m(fcm, current_raw)
        except Exception:
            log.exception("Error while processing kickoff_10m on first state init for match %s", match_id)

        # VAR 포인터 초기화(기존 유지)
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
        max_dis_id = int(mx["max_id"] or 0) if mx else 0

        execute(
            """
            UPDATE match_notification_state
            SET last_goal_disallowed_event_id = %s,
                updated_at = NOW()
            WHERE match_id = %s
            """,
            (max_dis_id, match_id),
        )

        # ✅ Goal 포인터/누적 스코어 초기화(필수)
        gx = fetch_one(
            """
            SELECT COALESCE(MAX(id), 0) AS max_id
            FROM match_events
            WHERE fixture_id = %s
              AND type = 'Goal'
            """,
            (match_id,),
        )
        max_goal_id = int(gx["max_id"] or 0) if gx else 0

        execute(
            """
            UPDATE match_notification_state
            SET last_goal_event_id = %s,
                last_goal_home_goals = %s,
                last_goal_away_goals = %s,
                updated_at = NOW()
            WHERE match_id = %s
            """,
            (max_goal_id, int(current_raw.home_goals), int(current_raw.away_goals), match_id),
        )

        return

    # ✅ 단조 status/red만 보정 (goals는 event 기반이라 max 금지)
    current = apply_monotonic_state(last, current_raw)

    # kickoff_10m (기존 유지)
    try:
        maybe_send_kickoff_10m(fcm, current)
    except Exception:
        log.exception("Error while processing kickoff_10m for match %s", match_id)

    # labels 1회 로딩
    labels = load_match_labels(match_id)
    home_id = labels.get("home_id")
    away_id = labels.get("away_id")

    # ==========================
    # ✅ 0) Goal 알림 (match_events 포인터 기반, OG 포함)
    # ==========================
    try:
        stg = fetch_one(
            """
            SELECT
              COALESCE(last_goal_event_id, 0) AS last_goal_event_id,
              COALESCE(last_goal_home_goals, 0) AS last_goal_home_goals,
              COALESCE(last_goal_away_goals, 0) AS last_goal_away_goals
            FROM match_notification_state
            WHERE match_id = %s
            """,
            (match_id,),
        )
        last_goal_id = int(stg["last_goal_event_id"] or 0) if stg else 0
        g_home = int(stg["last_goal_home_goals"] or 0) if stg else 0
        g_away = int(stg["last_goal_away_goals"] or 0) if stg else 0

        new_goals = fetch_all(
            """
            SELECT
              id,
              team_id,
              minute,
              COALESCE(extra, 0) AS extra,
              detail
            FROM match_events
            WHERE fixture_id = %s
              AND type = 'Goal'
              AND id > %s
            ORDER BY id ASC
            """,
            (match_id, last_goal_id),
        )

        invalid_markers = ("cancel", "disallow", "no goal", "offside", "foul", "annul", "null")

        for r in new_goals:
            ev_id = int(r["id"])
            team_id = int(r["team_id"]) if r.get("team_id") is not None else None
            minute = int(r["minute"] or 0) if r.get("minute") is not None else 0
            extra_min = int(r["extra"] or 0)
            detail = str(r.get("detail") or "")

            dlow = detail.lower()

            # ✅ (추가) cancel/disallow/no goal/offside 등 무효 마커가 붙은 Goal은 스킵 (OG는 예외)
            if any(m in dlow for m in invalid_markers) and ("own goal" not in dlow):
                execute(
                    """
                    UPDATE match_notification_state
                    SET last_goal_event_id = %s,
                        updated_at = NOW()
                    WHERE match_id = %s
                    """,
                    (ev_id, match_id),
                )
                last_goal_id = ev_id
                continue

            # ❌ 실축 PK 제외
            if ("missed penalty" in dlow) or (("miss" in dlow) and ("pen" in dlow)):
                execute(
                    """
                    UPDATE match_notification_state
                    SET last_goal_event_id = %s,
                        updated_at = NOW()
                    WHERE match_id = %s
                    """,
                    (ev_id, match_id),
                )
                last_goal_id = ev_id
                continue

            # ✅ 포인터 먼저 진전
            execute(
                """
                UPDATE match_notification_state
                SET last_goal_event_id = %s,
                    updated_at = NOW()
                WHERE match_id = %s
                """,
                (ev_id, match_id),
            )
            last_goal_id = ev_id

            # 득점 팀 판정(OG는 반대로)
            is_og = ("own goal" in dlow)
            inc_home = False
            inc_away = False

            if team_id is not None and home_id is not None and away_id is not None:
                if not is_og:
                    if int(team_id) == int(home_id):
                        inc_home = True
                    elif int(team_id) == int(away_id):
                        inc_away = True
                else:
                    if int(team_id) == int(home_id):
                        inc_away = True
                    elif int(team_id) == int(away_id):
                        inc_home = True

            old_home = g_home
            old_away = g_away

            if inc_home:
                g_home += 1
            elif inc_away:
                g_away += 1

            execute(
                """
                UPDATE match_notification_state
                SET last_goal_home_goals = %s,
                    last_goal_away_goals = %s,
                    updated_at = NOW()
                WHERE match_id = %s
                """,
                (g_home, g_away, match_id),
            )

            tokens = get_tokens_for_event(match_id, "score")
            if not tokens:
                continue

            goal_minute_str = f"{minute}+{extra_min}'" if extra_min else f"{minute}'"

            extra_payload = {
                "event_id": ev_id,
                "old_home": old_home,
                "old_away": old_away,
                "goal_minute_str": goal_minute_str,
                "goal_detail": detail,
                "goal_team_id": team_id,
            }

            score_state = MatchState(
                match_id=current.match_id,
                status=current.status,
                home_goals=g_home,
                away_goals=g_away,
                home_red=current.home_red,
                away_red=current.away_red,
            )

            title, body = build_message("score", score_state, extra_payload, labels)
            data: Dict[str, Any] = {"match_id": match_id, "event_type": "score"}
            data.update(extra_payload)

            batch_size = 500
            for i in range(0, len(tokens), batch_size):
                batch = tokens[i : i + batch_size]
                try:
                    resp = fcm.send_to_tokens(batch, title, body, data)
                    log.info("Sent score notification for match %s to %s devices: %s", match_id, len(batch), resp)
                except Exception:
                    log.exception("Failed to send score notification for match %s (event_id=%s)", match_id, ev_id)
                    break

    except Exception:
        log.exception("Error while processing goal(score) for match %s", match_id)

    # ==========================
    # ✅ VAR: Goal Disallowed 처리 (기존 로직 유지)
    # ==========================
    var_processed_ok = False
    try:
        st = fetch_one(
            """
            SELECT last_goal_disallowed_event_id
            FROM match_notification_state
            WHERE match_id = %s
            """,
            (match_id,),
        )
        if st:
            last_dis_id = int(st["last_goal_disallowed_event_id"] or 0)
            new_dis = load_new_goal_disallowed_events(match_id, last_dis_id)

            if new_dis:
                home_name = labels.get("home_name", "Home")
                away_name = labels.get("away_name", "Away")

                for ev in new_dis:
                    ev_id = int(ev["id"])
                    minute = int(ev.get("minute", 0) or 0)
                    extra_min = int(ev.get("extra", 0) or 0)
                    detail = str(ev.get("detail") or "")
                    team_id = ev.get("team_id")

                    minute_str = f"{minute}+{extra_min}'" if extra_min else f"{minute}'"

                    reason = None
                    if " - " in detail:
                        reason_raw = detail.split(" - ", 1)[1].strip()
                        if reason_raw:
                            reason = reason_raw[:1].upper() + reason_raw[1:]

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

                    if not tokens:
                        continue

                    title, body = build_message("goal_disallowed", current_raw, extra_payload, labels)
                    data: Dict[str, Any] = {"match_id": match_id, "event_type": "goal_disallowed"}
                    data.update(extra_payload)

                    batch_size = 500
                    for i in range(0, len(tokens), batch_size):
                        batch = tokens[i : i + batch_size]
                        try:
                            resp = fcm.send_to_tokens(batch, title, body, data)
                            log.info("Sent goal_disallowed notification for match %s to %s devices: %s", match_id, len(batch), resp)
                        except Exception:
                            log.exception("Failed to send goal_disallowed notification for match %s (event_id=%s)", match_id, ev_id)
                            break

        var_processed_ok = True
    except Exception:
        log.exception("Error while processing goal_disallowed for match %s", match_id)

    if var_processed_ok:
        try:
            execute(
                """
                UPDATE match_notification_state
                SET last_goal_home_goals = %s,
                    last_goal_away_goals = %s,
                    updated_at = NOW()
                WHERE match_id = %s
                """,
                (int(current_raw.home_goals), int(current_raw.away_goals), match_id),
            )
        except Exception:
            log.exception("Failed to reset last_goal_home/away_goals after VAR for match %s", match_id)

    # ==========================
    # 1) 나머지 단계/레드카드 이벤트(diff_events)
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

        if event_type == "redcard":
            red_time = load_last_redcard_minute(match_id)
            if red_time:
                minute = red_time.get("minute", 0)
                extra_min = red_time.get("extra", 0) or 0
                extra["red_minute_str"] = f"{minute}+{extra_min}'" if extra_min else f"{minute}'"

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

    ✅ 개선:
    - 워커 재시작(재배포) 직후 1회, "부트스트랩"으로
      match_notification_state(상태/포인터/단계 플래그)를 현재 시점으로 맞추고
      알림은 보내지 않는다.
    - 이렇게 하면 재배포 순간의 단계/골/VAR "알림 폭탄"이 사라지고,
      그 다음 루프부터는 정상적으로 "새 이벤트"만 알림이 간다.
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
            log.info("Bootstrap: syncing notification state for %s subscribed matches (no notifications).", len(matches))

        for match_id in matches:
            current_raw = load_current_match_state(match_id)
            if not current_raw:
                continue

            # state row 보장 + last_status/last_goals/last_red = 현재로 맞춤
            save_state(current_raw)

            # 포인터를 현재 MAX로 당겨서 과거 Goal/VAR를 new로 읽지 않게
            gx = fetch_one(
                """
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM match_events
                WHERE fixture_id = %s
                  AND type = 'Goal'
                """,
                (match_id,),
            )
            max_goal_id = int(gx["max_id"] or 0) if gx else 0

            vx = fetch_one(
                """
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM match_events
                WHERE fixture_id = %s
                  AND type = 'Var'
                  AND detail ILIKE 'Goal Disallowed%%'
                """,
                (match_id,),
            )
            max_dis_id = int(vx["max_id"] or 0) if vx else 0

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

            execute(
                """
                UPDATE match_notification_state
                SET
                  last_goal_event_id = %s,
                  last_goal_disallowed_event_id = %s,
                  last_goal_home_goals = %s,
                  last_goal_away_goals = %s,

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
                    max_goal_id,
                    max_dis_id,
                    int(current_raw.home_goals),
                    int(current_raw.away_goals),

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


# live_events_updater.py

from typing import List, Dict, Any

from db import fetch_all, execute
from live_fixtures_common import now_utc
from live_fixtures_a_group import (
    fetch_events_from_api,
    upsert_match_events,
    upsert_match_events_raw,
)


def _calc_score_from_events(
    events: List[Dict[str, Any]],
    home_team_id: int,
    away_team_id: int,
) -> tuple[int, int]:
    """
    events 리스트를 보고 홈/원정 득점 수를 계산.

    Api-Football 이벤트 구조에서:
      - type: "Goal"
      - detail: "Normal Goal", "Penalty", "Own Goal"
      - detail: "Missed Penalty", "Cancelled Goal" 등은 득점에서 제외

    여기서는:
      - type != "Goal"  → 무시
      - detail 에서 실축/취소/VAR 취소는 득점에서 제외
    """
    home_goals = 0
    away_goals = 0

    for ev in events:
        if not isinstance(ev, dict):
            continue

        type_ = (ev.get("type") or "").lower()
        detail = (ev.get("detail") or "").lower()

        # 1) 골 이벤트가 아니면 패스
        if type_ != "goal":
            continue

        # 2) 패널티 실축 / 골 취소 계열은 득점에서 제외
        #    (API 실제 detail 값에 맞춰 필요한 키워드는 더 추가 가능)
        if "missed" in detail and "penalty" in detail:
            # Missed Penalty
            continue
        if "cancel" in detail or "disallowed" in detail:
            # Cancelled goal / Disallowed goal
            continue
        if "var" in detail and ("no goal" in detail or "disallowed" in detail):
            # VAR: goal cancelled
            continue

        team_block = ev.get("team") or {}
        team_id = team_block.get("id")

        if team_id == home_team_id:
            home_goals += 1
        elif team_id == away_team_id:
            away_goals += 1

    return home_goals, away_goals



def update_live_scores_from_events() -> None:
    """
    1) DB 에서 현재 INPLAY 경기 목록 읽기
    2) 각 경기마다 /fixtures/events 호출
    3) match_events / match_events_raw 갱신
    4) events 기준으로 홈/원정 스코어 재계산해서 matches 테이블에 반영
    """
    now = now_utc()
    today = now.date().isoformat()

    rows = fetch_all(
        """
        SELECT fixture_id, home_id, away_id
        FROM matches
        WHERE status_group = 'INPLAY'
          AND DATE(date_utc) = %s
        """,
        (today,),
    )

    if not rows:
        return

    for r in rows:
        fixture_id = r.get("fixture_id")
        home_id = r.get("home_id")
        away_id = r.get("away_id")

        if fixture_id is None or home_id is None or away_id is None:
            continue

        try:
            # 1) Api-Football에서 이벤트 가져오기
            events = fetch_events_from_api(fixture_id)

            if not events:
                continue

            # 2) 이벤트 DB 반영
            upsert_match_events(fixture_id, events)
            upsert_match_events_raw(fixture_id, events)

            # 3) 이벤트 기준으로 스코어 재계산
            home_goals, away_goals = _calc_score_from_events(
                events,
                int(home_id),
                int(away_id),
            )

            # 4) 현재 DB 스코어와 비교 → 다를 때만 반영
            current = fetch_all(
                """
                SELECT home_ft, away_ft
                FROM matches
                WHERE fixture_id = %s
                  AND status_group = 'INPLAY'
                """,
                (fixture_id,),
            )

            if current:
                cur_home = current[0]["home_ft"] or 0
                cur_away = current[0]["away_ft"] or 0

                if (cur_home, cur_away) != (home_goals, away_goals):
                    execute(
                        """
                        UPDATE matches
                        SET home_ft = %s,
                            away_ft = %s
                        WHERE fixture_id = %s
                          AND status_group = 'INPLAY'
                        """,
                        (home_goals, away_goals, fixture_id),
                    )

            print(
                f"[events] fixture_id={fixture_id}: "
                f"{home_goals}-{away_goals} (from events)"
            )

        except Exception as e:
            # ❗ 경기 하나 실패해도 다음 경기 계속 돌게 하는 게 핵심
            print(
                f"[events] fixture_id={fixture_id} 처리 중 에러: {e}"
            )



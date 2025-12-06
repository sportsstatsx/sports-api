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
      - team.id: 득점 팀 ID
    만 사용해서 단순 카운트.
    """
    home_goals = 0
    away_goals = 0

    for ev in events:
        if not isinstance(ev, dict):
            continue

        type_ = (ev.get("type") or "").lower()
        if type_ != "goal":
            # 필요하면 나중에 Penalty, Own Goal 세부 처리 추가 가능
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

    # 오늘 날짜 + INPLAY 경기만 대상으로 최소한만 호출
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
                # 아직 이벤트가 하나도 없을 수도 있음 (0:0 진행 중)
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

            # 👉 추가: 이벤트 기준 스코어를 matches 에 반영 (라이브 경기만)
            try:
                # 현재 DB에 저장된 스코어와 비교해서, 달라질 때만 UPDATE
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
                    cur_home, cur_away = current[0]["home_ft"], current[0]["away_ft"]
                    # None 처리
                    if cur_home is None:
                        cur_home = 0
                    if cur_away is None:
                        cur_away = 0

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

            except Exception as e:
                print(
                    f"[events] fixture_id={fixture_id} 스코어 업데이트 중 에러: {e}"
                )

            print(
                f"[events] fixture_id={fixture_id}: "
                f"{home_goals}-{away_goals} (from events)"
            )


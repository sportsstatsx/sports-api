# matchdetail/timeline_block.py

from typing import Any, Dict, List
from db import fetch_all


def build_timeline_block(header: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    match_events 테이블을 기반으로 타임라인 이벤트 리스트를 만든다.

    - 지금은 확실히 존재하는 컬럼만 사용:
        fixture_id, team_id, type, detail, minute
    - 나중에 player_name, assist_name, time_extra 등 필요한 컬럼이
      확인되면 확장해서 넣으면 된다.
    """

    fixture_id = header["fixture_id"]
    home_id = header["home"]["id"]
    away_id = header["away"]["id"]

    rows = fetch_all(
        """
        SELECT
            e.minute,
            e.team_id,
            e.type,
            e.detail
        FROM match_events AS e
        WHERE e.fixture_id = %s
        ORDER BY e.minute ASC, e.id ASC
        """,
        (fixture_id,),
    )

    events: List[Dict[str, Any]] = []

    for r in rows:
        # 어떤 팀 이벤트인지 side 로 구분 (home / away)
        if r["team_id"] == home_id:
            side = "home"
        elif r["team_id"] == away_id:
            side = "away"
        else:
            # 혹시라도 팀ID가 둘 중 하나가 아니면 unknown 으로 처리
            side = "unknown"

        events.append(
            {
                "minute": r["minute"],
                "side": side,          # "home" / "away" / "unknown"
                "team_id": r["team_id"],
                "type": r["type"],     # "Goal" / "Card" / "subst" ...
                "detail": r["detail"], # "Normal Goal" / "Red Card" ...
            }
        )

    return events

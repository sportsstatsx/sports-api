from __future__ import annotations

from typing import Any, Dict, Optional


def insights_overall_goalsbytime(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    Insights Overall - Goals by Time 섹션 계산 자리.

    보통 데이터는:
      - stats["goals"]["for"]["minute"]
      - stats["goals"]["against"]["minute"]
    형태의 minute 버킷 구조를 사용하게 될 것이다.

    추후:
      - 각 구간(0–15, 16–30, …)별 for/against 수치 또는 비율을 계산해
        insights["goals_by_time"] 같은 구조로 채우는 로직을 작성하면 된다.
    """
    return

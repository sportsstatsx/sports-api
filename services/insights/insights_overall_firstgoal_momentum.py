from __future__ import annotations

from typing import Any, Dict, Optional


def insights_overall_firstgoal_momentum(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    Insights Overall - First Goal / Momentum 섹션용 자리.

    실제 계산은 현재
      services.insights.insights_overall_timing.insights_overall_timing
    안에서 Timing 지표와 함께 수행된다.

    (동일 이벤트를 두 번 조회하지 않기 위해 여기서는 별도 작업을 하지 않는다.)
    """
    return

# services/insights_overall_timing.py
from __future__ import annotations

from typing import Any, Dict, Optional


def insights_overall_timing(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    Timing 섹션 계산용 자리.

    현재는 서버 쪽 Timing 계산을 쓰지 않고,
    team_season_stats.value.insights_overall 에 이미 들어있는 데이터를
    그대로 사용하는 단계이므로, 여기서는 아무 것도 하지 않는다.
    """
    # TODO: match_events 기반 Timing 계산 로직을 구현하고 여기로 넣기
    return

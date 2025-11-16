# services/insights/insights_overall_firstgoal_momentum.py
from __future__ import annotations

from typing import Any, Dict, Optional


def enrich_overall_firstgoal_momentum(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
) -> None:
    """
    First Goal / Momentum 섹션용 자리만 잡아둔 함수.
    나중에 포아송이 아닌 실제 경기 기반 통계로 구현할 때 여기에 작성.
    """
    return

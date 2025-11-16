# services/insights/insights_overall_timing.py
from __future__ import annotations

from typing import Any, Dict, Optional


def enrich_overall_timing(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
) -> None:
    """
    Timing 섹션용 자리만 잡아둔 함수.
    나중에 로컬 DB 기준과 동일한 방식으로 구현할 때 여기에 작성.
    """
    return

# services/insights/insights_overall_discipline_setpieces.py
from __future__ import annotations

from typing import Any, Dict, Optional


def enrich_overall_discipline_setpieces(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
) -> None:
    """
    Discipline & Set Pieces 섹션용 자리만 잡아둔 함수.
    """
    return

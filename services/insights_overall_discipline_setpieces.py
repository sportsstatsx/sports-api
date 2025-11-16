# services/insights_overall_discipline_setpieces.py
from __future__ import annotations

from typing import Any, Dict, Optional


def insights_overall_discipline_setpieces(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    Discipline & Set Pieces 섹션 계산용 자리.
    아직 서버 쪽 구현은 안 되어 있고, team_season_stats.value 에 있는
    데이터를 그대로 사용하는 단계다.
    """
    # TODO: 카드 / 파울 / 세트피스 관련 계산 로직을 여기로 구현
    return

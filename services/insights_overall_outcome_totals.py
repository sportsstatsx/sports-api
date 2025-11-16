# services/insights_overall_outcome_totals.py
from __future__ import annotations

from typing import Any, Dict, Optional


def insights_overall_outcome_totals(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    Outcome & Totals 섹션 계산용 자리.

    지금 단계에서는 home_service.get_team_season_stats 안의 기존 로직을
    아직 그대로 사용하고 있기 때문에, 이 함수는 아무 작업도 하지 않는다.

    다음 단계에서:
      - get_team_season_stats 안에 있는 Outcome & Totals 관련 코드를
        전부 이 함수 안으로 옮길 예정.
    """
    # TODO: home_service.py 의 Outcome & Totals 계산 로직을 여기로 옮기기
    return

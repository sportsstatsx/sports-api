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
    Insights Overall - Discipline & Set Pieces 섹션 계산 자리.

    아직 서버 쪽 구현은 안 되어 있고,
    team_season_stats.value 에 있는 원시 데이터만 사용하는 상태라면
    여기서는 별도 작업을 하지 않아도 된다.

    추후:
      - 카드, 파울, 코너킥, 프리킥 등을 이용해
        insights_overall.* 형태로 가공하는 로직을 이 함수에 넣으면 된다.
    """
    return

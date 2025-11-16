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
    Insights Overall - Discipline & Set Pieces 섹션 계산용.

    예:
      - 옐로/레드 카드 평균
      - 파울, 프리킥, 코너 관련 지표 등

    지금은 team_season_stats.value.cards / corners 구조를
    그대로 쓰고 있을 가능성이 크다.

    다음 단계에서:
      - 카드/코너/세트피스 관련 계산을 이 함수 안에 정리해서
        insights_overall.* 키로 채워 넣을 예정.
    """
    # TODO: 카드 / 세트피스 관련 계산 로직을 여기로 구현
    return

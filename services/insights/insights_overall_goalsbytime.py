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
    Insights Overall - Goals by Time 섹션 계산용.

    예:
      - 0–15, 16–30, 31–45, 46–60, 61–75, 76–90, 91–105, 106–120
        구간별 득점/실점 카운트 또는 비율

    데이터 소스는 보통:
      - team_season_stats.value.goals.for.minute
      - team_season_stats.value.goals.against.minute
    와 같은 JSON 구조가 될 것이다.

    다음 단계에서:
      - 위 minute 버킷 정보를 기반으로
        Insights 탭의 Goals by Time 섹션에서 사용할 형태로
        가공하는 로직을 이 함수에 구현할 예정.
    """
    # TODO: goals.for.minute / goals.against.minute 를 이용해
    #       Goals by Time 섹션에 필요한 데이터를 계산하고
    #       insights 딕셔너리에 저장하는 로직을 구현
    return

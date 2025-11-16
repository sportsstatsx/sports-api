from __future__ import annotations

from typing import Any, Dict, Optional


def insights_overall_shooting_efficency(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    Insights Overall - Shooting & Efficiency 섹션 계산용.

    예:
      - shots_per_match (total/home/away)
      - shots_on_target_pct

    현재 home_service.get_team_season_stats 안에 있는
    match_team_stats 기반 슈팅 관련 쿼리/계산 블록을
    이 함수로 옮겨서 관리할 예정.
    """
    # TODO: 현재 home_service.py 안의 Shooting & Efficiency (Shots) 블록을
    #       이 함수 안으로 옮기기
    return

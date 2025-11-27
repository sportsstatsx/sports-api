# src/teamdetail/header_block.py

from __future__ import annotations
from typing import Dict, Any


def build_header_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail 상단 헤더 영역에 쓸 정보.

    TODO:
      - team 테이블 / standings / team_season_stats 등을 조인해서
        실제 팀명, 리그명, 순위, 승무패, 득/실점, 폼 등을 채워 넣자.
    """

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        # 아래 값들은 일단 None / 0 으로 두고, 나중에 실제 값 채우기
        "team_name": None,
        "team_short_name": None,
        "team_logo": None,
        "league_name": None,
        "season_label": str(season),
        "position": None,
        "played": 0,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goals_for": 0,
        "goals_against": 0,
        "goal_diff": 0,
        # 예: ["W", "D", "L", "W", "W"]
        "recent_form": [],
    }

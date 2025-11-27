# src/teamdetail/standing_block.py

from __future__ import annotations
from typing import Dict, Any


def build_standing_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    해당 팀이 속한 리그 standings에서 이 팀이 어떤 위치인지 보여주는 블록.

    TODO:
      - standings / team_season_stats 테이블 등을 사용해서
        그 리그 전체 standings를 가져오고,
        그 중에서 이 팀이 어디에 있는지 표시하자.
    """

    return {
        "league_id": league_id,
        "season": season,
        "team_id": team_id,
        # 전체 테이블(리그 테이블) – 일단 빈 리스트
        "table": [],
    }

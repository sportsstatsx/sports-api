# src/teamdetail/recent_results_block.py

from __future__ import annotations
from typing import Dict, Any, List


def build_recent_results_block(
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    최근 경기 결과 리스트 블록.

    TODO:
      - matches / fixtures 테이블에서 해당 팀이 홈/원정으로 출전한
        최신 N경기를 가져와서 채워 넣자.
    """

    rows: List[Dict[str, Any]] = []

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "rows": rows,
    }

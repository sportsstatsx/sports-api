# src/teamdetail/upcoming_block.py

from __future__ import annotations
from typing import Dict, Any, List


def build_upcoming_block(
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    앞으로 예정된 경기들(Upcoming Fixtures) 블록.

    TODO:
      - fixtures 테이블에서 status 가 'UPCOMING' / 'NS' 등인
        해당 팀의 경기들을 날짜순으로 가져오자.
    """

    upcoming_rows: List[Dict[str, Any]] = []

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "rows": upcoming_rows,
    }

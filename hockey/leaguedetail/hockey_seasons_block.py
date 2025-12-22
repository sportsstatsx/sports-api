# hockey/leaguedetail/hockey_seasons_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


def resolve_season_for_league(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    하키: 시즌 자동 선택
    1) hockey_league_seasons max(season)
    2) hockey_games max(season)
    3) hockey_standings max(season)
    """
    if season:
        return season

    row = hockey_fetch_one(
        """
        SELECT MAX(season) AS season
        FROM hockey_league_seasons
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row and row.get("season"):
        return int(row["season"])

    row = hockey_fetch_one(
        """
        SELECT MAX(season) AS season
        FROM hockey_games
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row and row.get("season"):
        return int(row["season"])

    row = hockey_fetch_one(
        """
        SELECT MAX(season) AS season
        FROM hockey_standings
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row and row.get("season"):
        return int(row["season"])

    return None


def build_hockey_seasons_block(league_id: int) -> Dict[str, Any]:

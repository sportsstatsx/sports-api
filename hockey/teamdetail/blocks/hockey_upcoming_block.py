# hockey/teamdetail/blocks/hockey_upcoming_block.py

from typing import List, Dict, Any
from hockey.hockey_db import hockey_fetch_all as fetch_all


def build_hockey_upcoming_block(
    *,
    team_id: int,
    league_id: int,
    season: int,
    limit: int,
) -> List[Dict[str, Any]]:

    return fetch_all(
        """
        SELECT *
        FROM hockey_games
        WHERE league_id = %(league_id)s
          AND season = %(season)s
          AND status = 'NS'
          AND (home_team_id = %(team_id)s OR away_team_id = %(team_id)s)
        ORDER BY game_date ASC
        LIMIT %(limit)s
        """,
        {
            "team_id": team_id,
            "league_id": league_id,
            "season": season,
            "limit": limit,
        },
    )

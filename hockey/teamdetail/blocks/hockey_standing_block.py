# hockey/teamdetail/blocks/hockey_standing_block.py

from typing import Dict, Any
from db import fetch_one


def build_hockey_standing_block(
    *,
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:

    row = fetch_one(
        """
        SELECT
            position,
            games_played,
            win_total,
            lose_total,
            goals_for,
            goals_against,
            points,
            form
        FROM hockey_standings
        WHERE team_id = %(team_id)s
          AND league_id = %(league_id)s
          AND season = %(season)s
        """,
        {
            "team_id": team_id,
            "league_id": league_id,
            "season": season,
        },
    )

    return row

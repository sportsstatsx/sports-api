# hockey/teamdetail/blocks/hockey_header_block.py

from typing import Dict, Any

from hockey.hockey_db import hockey_fetch_one as fetch_one


def build_hockey_header_block(
    *,
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:

    team = fetch_one(
        """
        SELECT id, name, logo
        FROM hockey_teams
        WHERE id = %(team_id)s
        """,
        {"team_id": team_id},
    )

    league = fetch_one(
        """
        SELECT id, name, logo
        FROM hockey_leagues
        WHERE id = %(league_id)s
        """,
        {"league_id": league_id},
    )

    return {
        "team": team,
        "league": league,
        "season": season,
    }

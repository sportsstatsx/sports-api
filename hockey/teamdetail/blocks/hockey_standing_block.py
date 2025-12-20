# hockey/teamdetail/blocks/hockey_standing_block.py

from typing import Dict, Any, List
from hockey.hockey_db import hockey_fetch_one as fetch_one
from hockey.hockey_db import hockey_fetch_all as fetch_all


def build_hockey_standing_block(
    *,
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:

    team_row = fetch_one(
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
        {"team_id": team_id, "league_id": league_id, "season": season},
    )

    table_rows: List[Dict[str, Any]] = fetch_all(
        """
        SELECT
            position,
            team_id,
            team_name,
            games_played,
            win_total,
            lose_total,
            goals_for,
            goals_against,
            points,
            form
        FROM hockey_standings
        WHERE league_id = %(league_id)s
          AND season = %(season)s
        ORDER BY position ASC NULLS LAST, points DESC NULLS LAST
        """,
        {"league_id": league_id, "season": season},
    )

    return {
        "team": team_row,
        "table": table_rows,
    }

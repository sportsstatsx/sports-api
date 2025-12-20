# hockey/teamdetail/blocks/hockey_overall_block.py

from typing import Dict, Any
from hockey.hockey_db import hockey_fetch_one as fetch_one


def build_hockey_overall_block(
    *,
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:

    row = fetch_one(
        """
        SELECT
            COUNT(*)                           AS games,
            SUM((score_json->>'home')::int)   AS goals_home,
            SUM((score_json->>'away')::int)  AS goals_away
        FROM hockey_games
        WHERE league_id = %(league_id)s
          AND season = %(season)s
          AND status = 'FT'
          AND (home_team_id = %(team_id)s OR away_team_id = %(team_id)s)
        """,
        {
            "team_id": team_id,
            "league_id": league_id,
            "season": season,
        },
    )

    return {
        "games": row["games"] or 0,
        "goals_for": (row["goals_home"] or 0) + (row["goals_away"] or 0),
        "goals_against": None,  # 축구와 동일 키 유지 (앱 계산용)
    }

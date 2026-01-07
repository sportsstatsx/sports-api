# hockey/teamdetail/blocks/hockey_upcoming_block.py

from typing import List, Dict, Any, Optional
from hockey.hockey_db import hockey_fetch_all as fetch_all


def build_hockey_upcoming_block(
    *,
    team_id: int,
    league_id: int,
    season: int,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:

    sql = """
        SELECT *
        FROM hockey_games
        WHERE league_id = %(league_id)s
          AND season = %(season)s
          AND status = 'NS'
          AND (home_team_id = %(team_id)s OR away_team_id = %(team_id)s)
        ORDER BY game_date ASC
    """

    params = {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
    }

    # ✅ limit이 주어졌을 때만 LIMIT 적용
    if limit is not None and int(limit) > 0:
        sql += "\n        LIMIT %(limit)s"
        params["limit"] = int(limit)

    return fetch_all(sql, params)

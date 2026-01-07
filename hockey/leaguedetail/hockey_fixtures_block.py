# hockey/leaguedetail/hockey_fixtures_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from hockey.hockey_db import hockey_fetch_all


def build_hockey_fixtures_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail - Fixtures 탭용
    - score_json home/away 둘 중 하나라도 NULL이면 예정/미확정으로 간주
    """
    rows_db = hockey_fetch_all(
        """
        SELECT
            g.id AS fixture_id,
            g.league_id AS league_id,
            g.season AS season,
            g.game_date AS date_utc,
            th.name AS home_team_name,
            ta.name AS away_team_name
        FROM hockey_games g
        JOIN hockey_teams th ON th.id = g.home_team_id
        JOIN hockey_teams ta ON ta.id = g.away_team_id
        WHERE g.league_id = %s
          AND g.season = %s
          AND (
            (g.score_json->>'home') IS NULL
            OR (g.score_json->>'away') IS NULL
          )
        ORDER BY g.game_date ASC
        LIMIT 200
        """,
        (league_id, season),
    )

    rows: List[Dict[str, Any]] = []
    for r in rows_db:
        dt = r.get("date_utc")
        if hasattr(dt, "isoformat"):
            r["date_utc"] = dt.isoformat()
        rows.append(
            {
                "fixture_id": r.get("fixture_id"),
                "league_id": r.get("league_id"),
                "season": r.get("season"),
                "date_utc": r.get("date_utc"),
                "home_team_name": r.get("home_team_name") or "",
                "away_team_name": r.get("away_team_name") or "",
            }
        )

    return {
        "league_id": league_id,
        "season": season,
        "matches": rows,
    }

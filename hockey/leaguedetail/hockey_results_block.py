# hockey/leaguedetail/hockey_results_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from hockey.hockey_db import hockey_fetch_all


def build_hockey_results_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail - Results 탭용
    - hockey_games에서 "완료 + 점수 확정"만
      (score_json->>'home' / 'away' 둘 다 NOT NULL)
    """
    rows_db = hockey_fetch_all(
        """
        SELECT
            g.id AS fixture_id,
            g.league_id AS league_id,
            g.season AS season,
            g.game_date AS date_utc,
            th.name AS home_team_name,
            ta.name AS away_team_name,
            (g.score_json->>'home')::int AS home_goals,
            (g.score_json->>'away')::int AS away_goals
        FROM hockey_games g
        JOIN hockey_teams th ON th.id = g.home_team_id
        JOIN hockey_teams ta ON ta.id = g.away_team_id
        WHERE g.league_id = %s
          AND g.season = %s
          AND (g.score_json->>'home') IS NOT NULL
          AND (g.score_json->>'away') IS NOT NULL
        ORDER BY g.game_date DESC
        LIMIT 100
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
                "home_goals": r.get("home_goals"),
                "away_goals": r.get("away_goals"),
            }
        )

    return {
        "league_id": league_id,
        "season": season,
        "matches": rows,
    }

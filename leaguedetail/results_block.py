# leaguedetail/results_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_results_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail 화면의 'Results' 탭 데이터.

    반환 형태(예시):
    {
        "league_id": 39,
        "season": 2025,
        "matches": [
            {
                "fixture_id": 1379085,
                "kickoff_time": "2025-02-10T20:00:00Z",
                "status_short": "FT",
                "home_team_id": 40,
                "home_team_name": "Liverpool",
                "home_team_logo": "...",
                "home_goals": 2,
                "away_team_id": 65,
                "away_team_name": "Chelsea",
                "away_team_logo": "...",
                "away_goals": 1,
            },
            ...
        ]
    }
    """
    rows: List[Dict[str, Any]] = []

    if season is None:
        # 시즌이 없으면 그냥 빈 리스트만 내려주자.
        return {
            "league_id": league_id,
            "season": season,
            "matches": [],
        }

    try:
        rows = fetch_all(
            """
            SELECT
                m.fixture_id,
                m.league_id,
                m.season,
                m.kickoff_time,
                m.status_short,
                m.home_team_id,
                th.name  AS home_team_name,
                th.logo  AS home_team_logo,
                m.away_team_id,
                ta.name  AS away_team_name,
                ta.logo  AS away_team_logo,
                m.goals_home,
                m.goals_away
            FROM matches m
            JOIN teams th ON th.id = m.home_team_id
            JOIN teams ta ON ta.id = m.away_team_id
            WHERE m.league_id = %s
              AND m.season = %s
              AND m.status_short IN ('FT','AET','PEN')
            ORDER BY m.kickoff_time DESC
            LIMIT 100
            """,
            (league_id, season),
        )
    except Exception as e:
        print(
            f"[build_results_block] ERROR league_id={league_id}, season={season}: {e}"
        )
        rows = []

    matches = [
        {
            "fixture_id": r.get("fixture_id"),
            "kickoff_time": r.get("kickoff_time"),
            "status_short": r.get("status_short"),
            "home_team_id": r.get("home_team_id"),
            "home_team_name": r.get("home_team_name"),
            "home_team_logo": r.get("home_team_logo"),
            "home_goals": r.get("goals_home"),
            "away_team_id": r.get("away_team_id"),
            "away_team_name": r.get("away_team_name"),
            "away_team_logo": r.get("away_team_logo"),
            "away_goals": r.get("goals_away"),
        }
        for r in rows
    ]

    return {
        "league_id": league_id,
        "season": season,
        "matches": matches,
    }

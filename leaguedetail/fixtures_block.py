# leaguedetail/fixtures_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_fixtures_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail 화면의 'Fixtures' 탭 데이터.

    - 기본은 해당 시즌의 '다가오는 경기'들만 내려주도록 구성.
    - status_short / kickoff_time 조건은 실제 DB 구조 맞춰서 조정하면 됨.
    """
    rows: List[Dict[str, Any]] = []

    if season is None:
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
                ta.logo  AS away_team_logo
            FROM matches m
            JOIN teams th ON th.id = m.home_team_id
            JOIN teams ta ON ta.id = m.away_team_id
            WHERE m.league_id = %s
              AND m.season = %s
              AND m.status_short IN ('NS','TBD','PST')
            ORDER BY m.kickoff_time ASC
            LIMIT 100
            """,
            (league_id, season),
        )
    except Exception as e:
        print(
            f"[build_fixtures_block] ERROR league_id={league_id}, season={season}: {e}"
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
            "away_team_id": r.get("away_team_id"),
            "away_team_name": r.get("away_team_name"),
            "away_team_logo": r.get("away_team_logo"),
        }
        for r in rows
    ]

    return {
        "league_id": league_id,
        "season": season,
        "matches": matches,
    }

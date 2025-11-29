# leaguedetail/standings_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_standings_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail 화면의 'Standings' 탭 데이터.

    여기서는 기본적인 standings만 내려주고,
    나중에 MLS / K리그 스플릿(컨퍼런스/그룹) 로직은
    이 블록 안에서 다 처리하면 됨.

    반환 예시:
    {
        "league_id": 39,
        "season": 2025,
        "rows": [
            {
                "rank": 1,
                "team_id": 40,
                "team_name": "Liverpool",
                "played": 20,
                "wins": 15,
                "draws": 3,
                "losses": 2,
                "goals_for": 45,
                "goals_against": 15,
                "points": 48,
                "group": null,
                "conference": null,
                "note": "Champions League"
            },
            ...
        ]
    }
    """
    if season is None:
        return {
            "league_id": league_id,
            "season": season,
            "rows": [],
        }

    rows: List[Dict[str, Any]] = []

    try:
        rows = fetch_all(
            """
            SELECT
                s.rank,
                s.team_id,
                t.name AS team_name,
                s.played,
                s.wins,
                s.draws,
                s.losses,
                s.goals_for,
                s.goals_against,
                s.points,
                s.group_name      AS group,
                s.conference_name AS conference,
                s.note
            FROM standings s
            JOIN teams t ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.season = %s
            ORDER BY s.rank ASC
            """,
            (league_id, season),
        )
    except Exception as e:
        print(
            f"[build_standings_block] ERROR league_id={league_id}, season={season}: {e}"
        )
        rows = []

    # 그대로 내려줘도 되고, 필요하면 여기서 필터링/그룹화
    return {
        "league_id": league_id,
        "season": season,
        "rows": rows,
    }

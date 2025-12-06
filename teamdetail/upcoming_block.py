# src/teamdetail/upcoming_block.py

from __future__ import annotations
from typing import Dict, Any, List

from db import fetch_all


def build_upcoming_block(
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    Team Detail 화면의 'Upcoming fixtures' 섹션에 내려줄 데이터.

    - recent_results_block 과 같은 matches / teams 스키마를 사용한다.
    - 차이점:
        * recent_results   : status_group = 'FT' 인 완료 경기들 (과거)
        * upcoming_fixtures: status_group != 'FT' 인 예정/진행 경기들 (미래 + 라이브)
    """

    rows_db = fetch_all(
        """
        SELECT
            m.fixture_id        AS fixture_id,       -- 각 경기의 fixture_id
            m.league_id         AS league_id,        -- 각 경기의 league_id (리그/컵 섞여 있음)
            m.season            AS season,
            m.date_utc          AS date_utc,
            m.home_id           AS home_team_id,
            m.away_id           AS away_team_id,
            th.name             AS home_team_name,
            ta.name             AS away_team_name
        FROM matches AS m
        JOIN teams   AS th ON th.id = m.home_id
        JOIN teams   AS ta ON ta.id = m.away_id
        WHERE m.season = %s
          AND (m.home_id = %s OR m.away_id = %s)
          -- ✅ FT가 아닌 경기들: NS, LIVE, HT, 기타 상태 포함
          AND m.status_group <> 'FT'
        ORDER BY m.date_utc ASC
        LIMIT 50
        """,
        (
            season,   # 1) WHERE m.season = %s
            team_id,  # 2) WHERE m.home_id = %s
            team_id,  # 3) WHERE m.away_id = %s
        ),
    )

    rows: List[Dict[str, Any]] = []

    for r in rows_db:
        date_utc = r["date_utc"]
        if hasattr(date_utc, "isoformat"):
            date_utc = date_utc.isoformat()

        rows.append(
            {
                "fixture_id": r["fixture_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "date_utc": date_utc,
                "home_team_id": r["home_team_id"],
                "away_team_id": r["away_team_id"],
                "home_team_name": r["home_team_name"],
                "away_team_name": r["away_team_name"],
                "league_name": None,
            }
        )

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "rows": rows,
    }

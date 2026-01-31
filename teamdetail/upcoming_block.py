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

    - ✅ 어떤 matchdetail(리그/대륙컵)에서 진입하든, 팀의 전체 경기(리그+대륙컵)를 모두 보여준다.
    - ✅ FINISHED 판정은 status_group / status / status_short 모두로 방어한다.

    NOTE:
    - league_id 파라미터는 번들 호환을 위해 유지하지만, upcoming 필터에는 사용하지 않는다.
    """

    rows_db = fetch_all(
        """
        SELECT
            m.fixture_id        AS fixture_id,
            m.league_id         AS league_id,
            m.season            AS season,
            m.date_utc          AS date_utc,
            m.home_id           AS home_team_id,
            m.away_id           AS away_team_id,
            th.name             AS home_team_name,
            ta.name             AS away_team_name
        FROM matches AS m
        JOIN teams   AS th ON th.id = m.home_id
        JOIN teams   AS ta ON ta.id = m.away_id
        WHERE m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND NOT (
            lower(coalesce(m.status_group,'')) = 'finished'
            OR coalesce(m.status,'') IN ('FT','AET','PEN')
            OR coalesce(m.status_short,'') IN ('FT','AET','PEN')
          )
        ORDER BY m.date_utc ASC
        LIMIT 50
        """,
        (
            season,
            team_id,
            team_id,
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
        "league_id": league_id,  # 번들 호환용(필터에는 미사용)
        "season": season,
        "rows": rows,
    }

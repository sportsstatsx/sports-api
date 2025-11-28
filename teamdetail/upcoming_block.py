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

    recent_results_block 과 같은 matches/teams 스키마를 사용하고,
    조건만 "앞으로 예정된 경기"로 바꾼다.

    ⚠️ 주의:
      - 일부 리그에서는 킥오프 전에도 home_ft / away_ft 를 0으로
        채워놓는 경우가 있어서, 여기서는 굳이 IS NULL 체크를 안 한다.
      - 단순히 date_utc 기준으로 "지금 이후" 경기만 가져오고,
        시즌 + 팀 ID로만 필터링한다.
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
        WHERE m.season = %s
          AND (m.home_id = %s OR m.away_id = %s)
          -- ✅ "앞으로 예정된 경기"만
          AND m.date_utc >= NOW()
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
                # league_name 은 matches 에 없으니 일단 생략 (null 로 내려감)
                "league_name": None,
            }
        )

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "rows": rows,
    }

# src/teamdetail/recent_results_block.py

from __future__ import annotations
from typing import Dict, Any, List

from db import fetch_all


def build_recent_results_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail 화면의 'Recent results' 섹션에 내려줄 데이터.

    - matches 테이블에서 해당 시즌, 해당 팀이 뛴 '완료된 경기'만 가져온다.
    - 리그 / 대륙컵 구분은 하지 않고, 단순히 최근 경기 리스트만 보여준다.
    - 앱 쪽 모델(RecentResultRow) 스키마에 맞춰서 JSON 필드를 구성한다.
    """

    rows_db = fetch_all(
        """
       SELECT
        NULL::bigint        AS fixture_id,
        m.league_id         AS league_id,   -- 🔥 각 경기의 진짜 league_id 사용
        m.season            AS season,      -- 시즌도 테이블 값 사용
        m.date_utc          AS date_utc,
        th.name             AS home_team_name,
        ta.name             AS away_team_name,
        m.home_ft           AS home_goals,
        m.away_ft           AS away_goals,

            CASE
                WHEN m.home_ft IS NULL OR m.away_ft IS NULL THEN NULL
                WHEN m.home_ft = m.away_ft THEN 'D'
                WHEN (m.home_id = %s AND m.home_ft > m.away_ft)
                  OR (m.away_id = %s AND m.away_ft > m.home_ft) THEN 'W'
                ELSE 'L'
            END                 AS result_code
        FROM matches AS m
        JOIN teams   AS th ON th.id = m.home_id
        JOIN teams   AS ta ON ta.id = m.away_id
        WHERE m.season = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND m.home_ft IS NOT NULL
          AND m.away_ft IS NOT NULL
        ORDER BY m.date_utc DESC
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
        # datetime 이면 문자열로 바꿔서 내려보내기 (앱은 String 으로 받음)
        if hasattr(date_utc, "isoformat"):
            date_utc = date_utc.isoformat()

        rows.append(
            {
                "fixture_id": r["fixture_id"],           # 지금은 항상 NULL
                "league_id": r["league_id"],
                "season": r["season"],
                "date_utc": date_utc,
                "home_team_name": r["home_team_name"],
                "away_team_name": r["away_team_name"],
                "home_goals": r["home_goals"],
                "away_goals": r["away_goals"],
                "result_code": r["result_code"],
            }
        )

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "rows": rows,
    }

# leaguedetail/results_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all
from leaguedetail.seasons_block import resolve_season_for_league


def build_results_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail 화면의 'Results' 탭 데이터.

    - matches 테이블에서 해당 리그 + 시즌의 '완료된 경기'만 가져온다.
    - Team Detail 의 recent_results_block 과 동일하게
      date_utc / home_ft / away_ft 기준으로 구성한다.
    - 앱에서는 LeagueDetailJsonParser 가 결과를 읽어서 Recent results 리스트를 만든다.

    최종 반환 형태 예시:
    {
        "league_id": 94,
        "season": 2025,
        "matches": [
            {
                "fixture_id": 123,
                "league_id": 94,
                "season": 2025,
                "date_utc": "2025-08-15T19:00:00",
                "home_team_name": "Ajax",
                "away_team_name": "PSV",
                "home_goals": 2,
                "away_goals": 1,
            },
            ...
        ]
    }
    """

    # 시즌이 전달 안 되면, 시즌 선택 로직에 맞춰 한 번 더 해석
    if season is None:
        season = resolve_season_for_league(league_id=league_id, season=None)

    rows_db: List[Dict[str, Any]] = []

    if season is not None:
        try:
            rows_db = fetch_all(
                """
                SELECT
                    m.fixture_id    AS fixture_id,       -- 각 경기의 fixture_id
                    m.league_id     AS league_id,
                    m.season        AS season,
                    m.date_utc      AS date_utc,
                    th.name         AS home_team_name,
                    ta.name         AS away_team_name,
                    m.home_ft       AS home_goals,
                    m.away_ft       AS away_goals
                FROM matches AS m
                JOIN teams   AS th ON th.id = m.home_id
                JOIN teams   AS ta ON ta.id = m.away_id
                WHERE m.league_id = %s
                  AND m.season    = %s
                  AND m.home_ft IS NOT NULL
                  AND m.away_ft IS NOT NULL
                ORDER BY m.date_utc DESC
                LIMIT 100
                """,
                (league_id, season),
            )
        except Exception as e:
            print(
                f"[build_results_block] ERROR league_id={league_id}, "
                f"season={season}: {e}"
            )
            rows_db = []

    rows: List[Dict[str, Any]] = []

    for r in rows_db:
        date_utc = r.get("date_utc")
        # datetime 이면 ISO 문자열로 변환
        if hasattr(date_utc, "isoformat"):
            date_utc = date_utc.isoformat()

        rows.append(
            {
                "fixture_id": r.get("fixture_id"),
                "league_id": r.get("league_id"),
                "season": r.get("season"),
                "date_utc": date_utc,
                "home_team_name": r.get("home_team_name"),
                "away_team_name": r.get("away_team_name"),
                "home_goals": r.get("home_goals"),
                "away_goals": r.get("away_goals"),
            }
        )

    return {
        "league_id": league_id,
        "season": season,
        "matches": rows,
    }

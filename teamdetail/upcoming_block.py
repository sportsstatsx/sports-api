# src/teamdetail/upcoming_block.py

from typing import Any, Dict, List
from db import fetch_all


def build_upcoming_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail 화면의 'Upcoming fixtures' 블록.

    - 기준: matches 테이블
    - 조건:
        * 해당 시즌
        * 내가 홈이거나 원정인 경기
        * 아직 끝나지 않은 경기 (home_ft/away_ft 가 NULL 이거나 status 가 예정 상태)
        * 현재 시각 이후 킥오프 (date_utc >= NOW())
    - 정렬: date_utc 오름차순
    """
    rows_sql = """
        SELECT
            m.fixture_id              AS fixture_id,
            m.league_id               AS league_id,
            m.season                  AS season,
            m.date_utc                AS date_utc,
            m.home_id                 AS home_team_id,
            m.away_id                 AS away_team_id,
            th.name                   AS home_team_name,
            ta.name                   AS away_team_name,
            l.name                    AS league_name
        FROM matches AS m
        JOIN teams   AS th ON th.id = m.home_id
        JOIN teams   AS ta ON ta.id = m.away_id
        JOIN leagues AS l  ON l.id  = m.league_id
        WHERE
            m.season = %s
            AND (m.home_id = %s OR m.away_id = %s)
            -- 이미 끝난 경기는 제외 (FT 스코어가 NULL 인 경기 = 아직 안 끝난 경기)
            AND (m.home_ft IS NULL OR m.away_ft IS NULL)
            -- 과거에 잡혀 있지만 이미 지난 킥오프는 제외
            AND m.date_utc >= NOW()
        ORDER BY
            m.date_utc ASC
        LIMIT 50;
    """

    rows_db: List[Dict[str, Any]] = fetch_all(
        rows_sql,
        (season, team_id, team_id),
    )

    rows: List[Dict[str, Any]] = []

    for r in rows_db:
        date_utc = r.get("date_utc")
        # psycopg2 timestamp → isoformat 문자열 변환
        if hasattr(date_utc, "isoformat"):
            date_utc = date_utc.isoformat()

        rows.append(
            {
                "fixture_id": r.get("fixture_id"),
                "league_id": r.get("league_id"),
                "season": r.get("season"),
                "date_utc": date_utc,
                "home_team_id": r.get("home_team_id"),
                "away_team_id": r.get("away_team_id"),
                "home_team_name": r.get("home_team_name"),
                "away_team_name": r.get("away_team_name"),
                "league_name": r.get("league_name"),
            }
        )

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "rows": rows,
    }

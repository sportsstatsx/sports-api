# src/teamdetail/upcoming_block.py

from __future__ import annotations

from typing import Dict, Any, List

from db import fetch_all  # ← header_block / recent_results_block 와 동일한 헬퍼 사용


def build_upcoming_block(
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    앞으로 예정된 경기들(Upcoming Fixtures) 블록.

    기준:
      - fixtures 테이블 기준
      - 해당 팀이 홈이거나 원정인 경기
      - 같은 season
      - 상태가 아직 시작 전인 경기들만 (NS / TBD / UPCOMING 등)
      - 날짜 오름차순 정렬
      - 최대 10경기 정도까지
    """

    sql = """
        SELECT
            f.fixture_id,
            f.league_id,
            f.season,
            f.date_utc,
            f.home_team_id,
            f.away_team_id,
            f.home_team_name,
            f.away_team_name,
            f.league_name
        FROM fixtures AS f
        WHERE
            (f.home_team_id = %(team_id)s OR f.away_team_id = %(team_id)s)
            AND f.season = %(season)s
            -- 필요하면 league_id 로 더 좁힐 수도 있음 (지금은 전체 대회 기준으로 둠)
            -- AND f.league_id = %(league_id)s
            AND f.status IN ('NS', 'TBD', 'UPCOMING')
        ORDER BY f.date_utc ASC
        LIMIT 10;
    """

    rows = fetch_all(
        sql,
        {
            "team_id": team_id,
            "season": season,
            "league_id": league_id,
        },
    )

    upcoming_rows: List[Dict[str, Any]] = []
    for r in rows:
        # date_utc 가 datetime 이라면 ISO 문자열로 변환
        date_utc = r.get("date_utc")
        if hasattr(date_utc, "isoformat"):
            date_utc_str = date_utc.isoformat()
        else:
            date_utc_str = date_utc

        upcoming_rows.append(
            {
                "fixture_id": r.get("fixture_id"),
                "league_id": r.get("league_id"),
                "season": r.get("season"),
                "date_utc": date_utc_str,
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
        "rows": upcoming_rows,
    }

# src/leaguedetail/fixtures_block.py
from __future__ import annotations
from typing import Any, Dict, List, Optional

from db import fetch_all


def build_fixtures_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail 화면의 'Fixtures' 탭 데이터.

    - 해당 리그 + 시즌의 '예정된 경기(아직 FT 스코어 없음)'만 내려준다.
    - Team Detail 의 upcoming_block 과 같은 기준을 리그 단위로 확장한 버전.
    """

    # 시즌이 정해지지 않았으면 그냥 빈 리스트 반환
    if season is None:
        return {
            "league_id": league_id,
            "season": season,
            "matches": [],
        }

    rows_db: List[Dict[str, Any]] = fetch_all(
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
        WHERE m.league_id = %s
          AND m.season    = %s
          -- ✅ TeamDetail upcoming 과 동일: 아직 FT 스코어가 없는 경기 = 예정/진행 중
          AND m.home_ft IS NULL
          AND m.away_ft IS NULL
        ORDER BY m.date_utc ASC
        LIMIT 200
        """,
        (
            league_id,  # 1) WHERE m.league_id = %s
            season,     # 2) WHERE m.season    = %s
        ),
    )

    matches: List[Dict[str, Any]] = []

    for r in rows_db:
        date_utc = r["date_utc"]
        # psycopg timestamp → ISO 문자열
        if hasattr(date_utc, "isoformat"):
            date_utc = date_utc.isoformat()

        matches.append(
            {
                "fixture_id": r["fixture_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "date_utc": date_utc,
                "home_team_id": r["home_team_id"],
                "away_team_id": r["away_team_id"],
                "home_team_name": r["home_team_name"],
                "away_team_name": r["away_team_name"],
                # 필요하면 나중에 league_name, 로고 등 추가 가능
            }
        )

    return {
        "league_id": league_id,
        "season": season,
        "matches": matches,
    }

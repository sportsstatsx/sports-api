# src/teamdetail/upcoming_block.py

from __future__ import annotations
from typing import Dict, Any, List

from db import fetch_all  # 기존 다른 블록들과 동일한 헬퍼 사용한다고 가정


def build_upcoming_block(
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    앞으로 예정된 경기들(Upcoming Fixtures) 블록.

    - fixtures 테이블에서 status 가 'UPCOMING' / 'NS' / 'TBD' 인
      해당 팀의 경기를 날짜 오름차순으로 가져온다.
    - 한 시즌 기준으로만 조회.
    - 리그는 한 팀이 여러 대회에 나갈 수 있으니까 league_id 로는
      WHERE 를 걸지 않고, 각 row 에 league_name 을 같이 내려준다.
    """

    upcoming_rows: List[Dict[str, Any]] = []

    sql = """
        SELECT
            f.fixture_id,
            f.league_id,
            f.season,
            f.date_utc,
            f.home_team_id,
            f.away_team_id,
            th.name AS home_team_name,
            ta.name AS away_team_name,
            COALESCE(l.short_name, l.name) AS league_name
        FROM fixtures AS f
        JOIN teams   AS th ON th.id = f.home_team_id
        JOIN teams   AS ta ON ta.id = f.away_team_id
        JOIN leagues AS l  ON l.id = f.league_id
        WHERE
            (f.home_team_id = %(team_id)s OR f.away_team_id = %(team_id)s)
            AND f.season = %(season)s
            AND f.status IN ('NS', 'TBD', 'UPCOMING')
        ORDER BY f.date_utc ASC
        LIMIT 20
    """

    try:
        rows = fetch_all(
            sql,
            {
                "team_id": team_id,
                "season": season,
            },
        )
    except Exception as e:
        # 문제 생겨도 전체 번들이 죽지 않도록 하고, 서버 로그만 남김
        print(f"[teamdetail.upcoming_block] DB error: {e}")
        rows = []

    for r in rows:
        dt = r.get("date_utc")
        if dt is not None:
            # psycopg2 RealDictCursor 기준: datetime → ISO 문자열
            date_utc_str = dt.isoformat()
        else:
            date_utc_str = None

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

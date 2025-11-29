# leaguedetail/fixtures_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_fixtures_block(league_id: int, season: Optional[int]) -> Dict[str, Any]:
    """
    League Detail 화면의 'Fixtures' 탭 데이터.

    - 해당 리그 + 시즌의 '다가오는 경기'만 내려준다.
    - 아직 시작하지 않은 경기 = kickoff_time 이 지금 이후인 경기라고 가정한다.
    """

    rows: List[Dict[str, Any]] = []

    # 시즌이 정해지지 않았으면 아무 것도 내려줄 수 없음
    if season is None:
        return {
            "league_id": league_id,
            "season": season,
            "matches": [],
        }

    try:
        # ⚽ matches 테이블에서
        #  - league_id / season 필터
        #  - kickoff_time 이 현재 시각 이후(미래 경기)
        #  기준으로 가져온다.
        #
        #  status_short 같은 컬럼을 안 쓰고,
        #  단순히 kickoff_time 기준으로만 "다가오는 경기"를 정의해서
        #  컬럼 이름 문제로 인한 오류를 피한다.
        rows = fetch_all(
            """
            SELECT
                fixture_id,
                league_id,
                season,
                kickoff_time,
                home_team_id,
                home_team_name,
                home_team_logo,
                away_team_id,
                away_team_name,
                away_team_logo,
                home_goals,
                away_goals
            FROM matches
            WHERE league_id = %s
              AND season    = %s
              AND kickoff_time >= NOW()
            ORDER BY kickoff_time ASC
            LIMIT 200
            """,
            (league_id, season),
        )

    except Exception as e:
        print(f"[build_fixtures_block] ERROR league_id={league_id}, season={season}: {e}")
        rows = []

    # 앱에서 쓰기 좋은 형태로 필드 매핑
    matches: List[Dict[str, Any]] = [
        {
            "fixture_id": r.get("fixture_id"),
            "kickoff_time": r.get("kickoff_time"),
            "league_id": r.get("league_id"),
            "season": r.get("season"),
            "home_team_id": r.get("home_team_id"),
            "home_team_name": r.get("home_team_name"),
            "home_team_logo": r.get("home_team_logo"),
            "away_team_id": r.get("away_team_id"),
            "away_team_name": r.get("away_team_name"),
            "away_team_logo": r.get("away_team_logo"),
            # 미래 경기라 득점은 실제로는 안 쓰지만,
            # 혹시 모를 사용을 위해 그대로 내려준다.
            "home_goals": r.get("home_goals"),
            "away_goals": r.get("away_goals"),
        }
        for r in rows
    ]

    return {
        "league_id": league_id,
        "season": season,
        "matches": matches,
    }

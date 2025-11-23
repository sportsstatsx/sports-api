# services/matchdetail/header_block.py

from typing import Any, Dict, Optional
from db import fetch_one


def build_header_block(
    fixture_id: int,
    league_id: int,
    season: int,
) -> Optional[Dict[str, Any]]:
    """
    matches 테이블에서 매치디테일 상단에 필요한 정보만 추출해서 header 블록을 만든다.

    컬럼 이름은 지금 네가 쓰던 스타일 기준으로 가정:
      - fixture_id, league_id, season
      - date_utc, status_short, elapsed
      - home_id, home_name, home_logo, home_goals, home_red_cards
      - away_id, away_name, away_logo, away_goals, away_red_cards
    """

    row = fetch_one(
        """
        SELECT
          m.fixture_id,
          m.league_id,
          m.season,
          m.date_utc,
          m.status_short,
          m.elapsed,
          m.home_id,
          m.home_name,
          m.home_logo,
          m.home_goals,
          m.home_red_cards,
          m.away_id,
          m.away_name,
          m.away_logo,
          m.away_goals,
          m.away_red_cards
        FROM matches AS m
        WHERE m.fixture_id = %s
          AND m.league_id = %s
          AND m.season = %s
        """,
        (fixture_id, league_id, season),
    )

    if row is None:
        return None

    return {
        "fixture_id": row["fixture_id"],
        "league_id": row["league_id"],
        "season": row["season"],
        "kickoff_utc": row["date_utc"],   # 앱에서 로컬 타임으로 변환
        "status": row["status_short"],    # NS / 1H / HT / 2H / FT ...
        "minute": row["elapsed"],         # 진행 중일 때만 의미

        "home": {
            "id": row["home_id"],
            "name": row["home_name"],
            "short_name": row["home_name"],  # 나중에 약칭 컬럼 있으면 거기로 교체
            "logo": row["home_logo"],
            "score": row["home_goals"],
            "red_cards": row["home_red_cards"],
        },
        "away": {
            "id": row["away_id"],
            "name": row["away_name"],
            "short_name": row["away_name"],
            "logo": row["away_logo"],
            "score": row["away_goals"],
            "red_cards": row["away_red_cards"],
        },
    }

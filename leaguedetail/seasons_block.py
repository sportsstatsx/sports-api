# leaguedetail/seasons_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_seasons_block(league_id: int) -> Dict[str, Any]:
    """
    League Detail 화면의 'Seasons' 탭 + 기본 시즌 선택에 사용할 시즌 목록.

    반환 형태 예시:
    {
        "league_id": 39,
        "seasons": [2025, 2024, 2023]
    }
    """
    seasons: List[int] = []

    try:
        rows = fetch_all(
            """
            SELECT DISTINCT season
            FROM matches
            WHERE league_id = %s
            ORDER BY season DESC
            """,
            (league_id,),
        )
        seasons = [int(r["season"]) for r in rows if r.get("season") is not None]
    except Exception as e:
        print(f"[build_seasons_block] ERROR league_id={league_id}: {e}")
        seasons = []

    return {
        "league_id": league_id,
        "seasons": seasons,
    }


def resolve_season_for_league(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    쿼리에서 season이 안 넘어오면, 해당 리그의 최신 시즌을 골라주는 헬퍼.
    쿼리에서 season이 있으면 그대로 사용.
    """
    if season is not None:
        return season

    try:
        rows = fetch_all(
            """
            SELECT MAX(season) AS max_season
            FROM matches
            WHERE league_id = %s
            """,
            (league_id,),
        )
        if rows:
            max_season = rows[0].get("max_season")
            if max_season is not None:
                return int(max_season)
    except Exception as e:
        print(f"[resolve_season_for_league] ERROR league_id={league_id}: {e}")

    # 시즌 정보가 전혀 없을 경우
    return None

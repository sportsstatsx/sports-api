from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_seasons_block(league_id: int) -> Dict[str, Any]:
    """
    League Detail 화면의 'Seasons' 탭 + 기본 시즌 선택에 사용할 시즌 목록.

    반환 형태 예시:
    {
        "league_id": 39,
        "seasons": [2025, 2024, 2023],
        "season_champions": [
            {"season": 2025, "team_id": 40, "team_name": "Arsenal", "points": 89},
            ...
        ]
    }
    """
    seasons: List[int] = []
    season_champions: List[Dict[str, Any]] = []

    # 1) 사용 가능한 시즌 목록
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

    # 2) 시즌별 우승 팀 (standings 테이블에서 position=1 기준)
    #    테이블/컬럼 구조가 다르면 이 쿼리는 실패하고, season_champions 는 빈 리스트로 남는다.
    try:
        champ_rows = fetch_all(
            """
            SELECT season, team_id, team_name, points
            FROM standings
            WHERE league_id = %s
              AND position = 1
            ORDER BY season DESC
            """,
            (league_id,),
        )
        season_champions = []
        for r in champ_rows:
            season_val = r.get("season")
            if season_val is None:
                continue
            season_champions.append(
                {
                    "season": int(season_val),
                    "team_id": r.get("team_id"),
                    "team_name": r.get("team_name") or "",
                    "points": r.get("points"),
                }
            )
    except Exception as e:
        print(f"[build_seasons_block] CHAMPIONS ERROR league_id={league_id}: {e}")
        season_champions = []

    return {
        "league_id": league_id,
        "seasons": seasons,
        # 🔹 시즌별 우승 팀 정보 (앱에서 Season 탭에서 사용)
        "season_champions": season_champions,
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

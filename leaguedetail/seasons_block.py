from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_seasons_block(league_id: int) -> Dict[str, Any]:
    """
    League Detail 화면의 'Seasons' 탭 + 기본 시즌 선택에 사용할 시즌 목록.

    반환 예시:
    {
        "league_id": 188,
        "seasons": [2025, 2024],
        "season_champions": [
            {"season": 2025, "team_id": 943, "team_name": "Some Club", "points": 12},
            {"season": 2024, "team_id": 24608, "team_name": "Another Club", "points": 53}
        ]
    }
    """
    seasons: List[int] = []
    season_champions: List[Dict[str, Any]] = []

    # 1) 사용 가능한 시즌 목록 (matches 기준)
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

    # 2) 시즌별 우승 팀 (standings 기준)
    #    - league_id = X
    #    - rank = 1
    #    - 같은 시즌에 여러 group_name 이 있을 수 있으니
    #      → DISTINCT ON (season) 으로 시즌당 한 팀만 선택
    try:
        champ_rows = fetch_all(
            """
            SELECT DISTINCT ON (s.season)
                s.season,
                s.team_id,
                COALESCE(t.name, '') AS team_name,
                s.points
            FROM standings AS s
            LEFT JOIN teams AS t
              ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.rank = 1
            ORDER BY s.season DESC, s.rank ASC;
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

    # 3) 현재 진행 중인 시즌(가장 최신 시즌)은 챔피언 목록에서 제외
    try:
        latest_season = resolve_season_for_league(league_id, None)
    except Exception as e:
        latest_season = None
        print(
            f"[build_seasons_block] resolve_season_for_league ERROR league_id={league_id}: {e}"
        )

    if latest_season is not None and len(season_champions) > 1:
        season_champions = [
            c for c in season_champions
            if c.get("season") != latest_season
        ]

    return {
        "league_id": league_id,
        "seasons": seasons,
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

    return None

# hockey/leaguedetail/hockey_seasons_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


def resolve_season_for_league(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    하키: 시즌 자동 선택
    1) hockey_league_seasons max(season)
    2) hockey_games max(season)
    3) hockey_standings max(season)
    """
    if season:
        return season

    row = hockey_fetch_one(
        """
        SELECT MAX(season) AS season
        FROM hockey_league_seasons
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row and row.get("season"):
        return int(row["season"])

    row = hockey_fetch_one(
        """
        SELECT MAX(season) AS season
        FROM hockey_games
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row and row.get("season"):
        return int(row["season"])

    row = hockey_fetch_one(
        """
        SELECT MAX(season) AS season
        FROM hockey_standings
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row and row.get("season"):
        return int(row["season"])

    return None


def build_hockey_seasons_block(league_id: int) -> Dict[str, Any]:
    """
    seasons:
      - hockey_league_seasons 기준 DESC

    season_champions:
      - 완료 시즌(최신 시즌 제외)
      - hockey_standings 에서 position = 1
      - playoff/final stage 우선
    """
    rows = hockey_fetch_all(
        """
        SELECT season
        FROM hockey_league_seasons
        WHERE league_id = %s
        ORDER BY season DESC
        """,
        (league_id,),
    )

    seasons: List[int] = []
    for r in rows:
        s = r.get("season")
        if isinstance(s, int):
            seasons.append(s)

    current_season = seasons[0] if seasons else None
    season_champions: List[Dict[str, Any]] = []

    for season in seasons:
        if season == current_season:
            continue  # 현재 시즌 제외

        # position = 1 후보들
        candidates = hockey_fetch_all(
            """
            SELECT
              hs.team_id,
              hs.points,
              hs.stage,
              hs.group_name
            FROM hockey_standings hs
            WHERE hs.league_id = %s
              AND hs.season = %s
              AND hs.position = 1
            """,
            (league_id, season),
        )

        if not candidates:
            continue

        def score(row: Dict[str, Any]) -> int:
            stage = (row.get("stage") or "").lower()
            group = (row.get("group_name") or "").lower()
            pts = row.get("points") or 0

            s = 0
            if "playoff" in stage:
                s += 1000
            if "final" in stage:
                s += 800
            if "overall" in group:
                s += 200
            s += int(pts)
            return s

        best = sorted(candidates, key=score, reverse=True)[0]

        team = hockey_fetch_one(
            """
            SELECT name, logo
            FROM hockey_teams
            WHERE id = %s
            """,
            (best["team_id"],),
        ) or {}

        season_champions.append(
            {
                "season_label": str(season),
                "champion": {
                    "id": best["team_id"],
                    "name": team.get("name", ""),
                    "logo": team.get("logo"),
                },
                "note": f"Points: {best['points']}"
                if best.get("points") is not None
                else None,
            }
        )

    return {
        "league_id": league_id,
        "seasons": seasons,
        "season_champions": season_champions,
    }


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
    앱 LeagueDetailJsonParser가 기대하는 형태를 축구와 비슷하게 맞춤:
      - seasons_block: { "league_id":..., "seasons":[...], "season_champions":[...] }

    구현 방침:
    - seasons: hockey_league_seasons에서 DESC
    - season_champions: "완료 시즌"에 대해(= 최신 시즌 제외),
      hockey_standings + hockey_teams 기반으로 best-effort 챔피언 1팀을 구성한다.
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
        if s is None:
            continue
        try:
            seasons.append(int(s))
        except Exception:
            continue

    # 최신 시즌(현재 시즌) 제외
    current_season: Optional[int] = seasons[0] if seasons else None

    season_champions: List[Dict[str, Any]] = []

    # 완료 시즌들에 대해 챔피언 후보 1팀 생성
    for s in seasons:
        if current_season is not None and s == current_season:
            continue

        # 1) hockey_standings에서 rank=1 후보들 중 "playoff/final" 같은 stage 우선
        cand_rows = hockey_fetch_all(
            """
            SELECT
              hs.team_id AS team_id,
              hs.points  AS points,
              hs.stage   AS stage,
              hs.group_name AS group_name,
              hs.rank    AS rank
            FROM hockey_standings hs
            WHERE hs.league_id = %s
              AND hs.season = %s
              AND hs.rank = 1
            """,
            (league_id, s),
        )

        # 후보 없으면 skip (그 시즌 스탠딩 자체가 없다는 뜻)
        if not cand_rows:
            continue

        def _score_candidate(r: Dict[str, Any]) -> int:
            """
            stage/group 힌트를 기반으로 우승 후보 우선순위를 만든다.
            - playoff/final 포함 stage 우선
            - group_name에 overall/final 포함이면 가산
            - points 높은 팀 가산(동점이면 의미 없음)
            """
            stage = (r.get("stage") or "").lower()
            group_name = (r.get("group_name") or "").lower()
            points = r.get("points")
            p = int(points) if isinstance(points, (int, float)) else 0

            score = 0
            if "playoff" in stage or "playoffs" in stage:
                score += 1000
            if "final" in stage:
                score += 900
            if "overall" in group_name:
                score += 200
            if "final" in group_name:
                score += 150

            # points 보조 점수
            score += p
            return score

        best = sorted(cand_rows, key=_score_candidate, reverse=True)[0]
        team_id = best.get("team_id")
        if team_id is None:
            continue

        team = hockey_fetch_one(
            """
            SELECT name, logo
            FROM hockey_teams
            WHERE id = %s
            LIMIT 1
            """,
            (team_id,),
        ) or {}

        team_name = (team.get("name") or "").strip()
        team_logo = (team.get("logo") or "").strip() or None

        # note는 축구처럼 "Points: X" 형태(없으면 None)
        pts = best.get("points")
        note: Optional[str] = None
        if isinstance(pts, (int, float)):
            note = f"Points: {int(pts)}"

        season_champions.append(
            {
                "season_label": str(s),
                "champion": {
                    "id": int(team_id) if team_id is not None else None,
                    "name": team_name,
                    "logo": team_logo,
                },
                "note": note,
            }
        )

    return {
        "league_id": league_id,
        "seasons": seasons,
        "season_champions": season_champions,
    }


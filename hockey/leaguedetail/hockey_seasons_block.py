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

    season_champions (중요 정책):
      1) ✅ 브라켓/플레이오프가 있는 리그는 "Final winner"가 챔피언
         - hockey_tournament_ties 에서 최종 라운드(=Final 계열) winner_team_id 우선
      2) 브라켓 데이터가 없거나 winner가 없으면 standings(position=1) fallback
         - 단, 'overall(정규시즌 1위)' 가산점으로 챔피언이 뒤집히지 않게 보수적으로 선택
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

    def _pick_champion_from_ties(league_id: int, season: int) -> Optional[int]:
        """
        hockey_tournament_ties 에서 Final winner 우선 추출.
        - round/round_name 표기 변형이 많아서 'final' 포함 여부 기반으로 점수화
        """
        rows = hockey_fetch_all(
            """
            SELECT
              winner_team_id,
              COALESCE(round_name, round, '') AS rnd
            FROM hockey_tournament_ties
            WHERE league_id = %s
              AND season = %s
              AND winner_team_id IS NOT NULL
            """,
            (league_id, season),
        )

        if not rows:
            return None

        def _tie_score(r: Dict[str, Any]) -> int:
            rnd = (r.get("rnd") or "").lower()
            s = 0

            # ✅ Final 최우선 (semi/final 혼동 방지)
            if "final" in rnd:
                s += 2000
                if "semi" in rnd:
                    s -= 1500  # 'semi-final'이면 Final로 보지 않게 강하게 패널티

            # 컨퍼런스/리그 파이널 같은 변형도 결국 final 포함이므로 위에서 커버
            # 최종결정전이 아닌 것들(quarter 등)은 점수 낮음
            if "semi" in rnd:
                s += 400
            if "quarter" in rnd:
                s += 200
            if "round of" in rnd:
                s += 100

            return s

        rows_sorted = sorted(rows, key=_tie_score, reverse=True)
        best = rows_sorted[0]
        return int(best["winner_team_id"]) if best.get("winner_team_id") is not None else None

    for season in seasons:
        if season == current_season:
            continue  # 현재 시즌 제외

        # 1) ✅ 브라켓 Final winner 우선
        champion_team_id = _pick_champion_from_ties(league_id, season)

        # 2) fallback: standings position=1
        note: Optional[str] = None
        if champion_team_id is None:
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

            def _standings_score(row: Dict[str, Any]) -> int:
                stage = (row.get("stage") or "").lower()
                group = (row.get("group_name") or "").lower()
                pts = row.get("points") or 0

                s = 0
                # ✅ playoff/final 관련이면 크게 가산
                if "playoff" in stage or "playoffs" in stage:
                    s += 2000
                if "final" in stage:
                    s += 1500

                # ✅ group_name 'overall'은 "정규시즌 1위" 가능성이 높으니
                #    챔피언 판정에 유리하게 주지 않음(오히려 약하게)
                if "overall" in group:
                    s += 10

                # 동점/복수 후보 중 안정적인 결정용으로만 points 반영(가중치 낮게)
                try:
                    s += int(pts)
                except Exception:
                    pass
                return s

            best = sorted(candidates, key=_standings_score, reverse=True)[0]
            champion_team_id = int(best["team_id"])
            if best.get("points") is not None:
                note = f"Points: {best['points']}"

        team = hockey_fetch_one(
            """
            SELECT name, logo
            FROM hockey_teams
            WHERE id = %s
            """,
            (champion_team_id,),
        ) or {}

        season_champions.append(
            {
                "season_label": str(season),
                "champion": {
                    "id": champion_team_id,
                    "name": team.get("name", ""),
                    "logo": team.get("logo"),
                },
                "note": note,
            }
        )

    return {
        "league_id": league_id,
        "seasons": seasons,
        "season_champions": season_champions,
    }



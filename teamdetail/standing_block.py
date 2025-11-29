# src/teamdetail/standing_block.py

from __future__ import annotations
from typing import Dict, Any, List

from db import fetch_all


def build_standing_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    해당 팀이 속한 리그 standings에서 이 팀이 어떤 위치인지 보여주는 블록.

    - standings 테이블(또는 동일 스키마)을 사용해서
      해당 league_id / season 의 전체 리그 테이블을 가져온다.
    - 그 전체 테이블을 그대로 내려주면,
      앱 쪽에서 이 팀 한 줄만 보여주거나, Show all 로 전체를 펼치는 방식으로 사용한다.
    """

    # league_id / season 이 없으면 그냥 빈 껍데기 반환
    if not league_id or not season:
        return {
            "league_id": league_id,
            "season": season,
            "team_id": team_id,
            "table": [],
        }

    try:
        rows: List[Dict[str, Any]] = fetch_all(
            """
            SELECT
                s.rank,
                s.team_id,
                t.name       AS team_name,
                s.played,
                s.win        AS wins,
                s.draw       AS draws,
                s.lose       AS losses,
                s.goals_for,
                s.goals_against,
                s.goals_diff AS goal_diff,
                s.points
            FROM standings AS s
            JOIN teams     AS t ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.season    = %s
            ORDER BY s.rank ASC
            """,
            (league_id, season),
        )
    except Exception:
        # 여기서 에러 나더라도 Team Detail 전체가 죽지 않도록
        return {
            "league_id": league_id,
            "season": season,
            "team_id": team_id,
            "table": [],
        }

    if not rows:
        return {
            "league_id": league_id,
            "season": season,
            "team_id": team_id,
            "table": [],
        }

    def _coalesce_int(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    table: List[Dict[str, Any]] = []
    for r in rows:
        table.append(
            {
                # Kotlin StandingRow.position
                "position": _coalesce_int(r.get("rank"), 0),
                # Kotlin StandingRow.teamId
                "team_id": _coalesce_int(r.get("team_id"), 0),
                # Kotlin StandingRow.teamName
                "team_name": r.get("team_name") or "",
                # 이하: played / wins / draws / losses / goals_for / goals_against / goal_diff / points
                "played": _coalesce_int(r.get("played"), 0),
                "wins": _coalesce_int(r.get("wins"), 0),
                "draws": _coalesce_int(r.get("draws"), 0),
                "losses": _coalesce_int(r.get("losses"), 0),
                "goals_for": _coalesce_int(r.get("goals_for"), 0),
                "goals_against": _coalesce_int(r.get("goals_against"), 0),
                "goal_diff": _coalesce_int(r.get("goal_diff"), 0),
                "points": _coalesce_int(r.get("points"), 0),
            }
        )

    return {
        "league_id": int(league_id),
        "season": int(season),
        "team_id": int(team_id),
        "table": table,
    }

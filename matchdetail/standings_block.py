# services/matchdetail/standings_block.py

from typing import Any, Dict, Optional, List

from db import fetch_all


def build_standings_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Match Detail용 Standings 블록.

    - header 블록에 있는 league_id / season / home.id / away.id 기준으로
      standings 테이블 + teams 테이블을 조회해서 리그 테이블을 만든다.
    - JSON 형태는 대략:

      "standings": {
        "league": {
          "league_id": 39,
          "season": 2025,
          "name": "Premier League"
        },
        "rows": [
          {
            "position": 1,
            "team_id": 40,
            "team_name": "Liverpool",
            "team_logo": "https://...",
            "played": 10,
            "win": 8,
            "draw": 1,
            "loss": 1,
            "goals_for": 24,
            "goals_against": 8,
            "goal_diff": 16,
            "points": 25,
            "description": "Champions League",
            "group_name": "Overall",
            "form": "W W D W W",
            "is_home": false,
            "is_away": true
          },
          ...
        ]
      }
    """

    league_id = header.get("league_id")
    season = header.get("season")

    # header.league.name (Kotlin HeaderBlock 기준)
    league_name = None
    league_info = header.get("league") or {}
    if isinstance(league_info, dict):
        league_name = league_info.get("name")

    # header.home.id / header.away.id (Kotlin TeamSideHeader 기준)
    def _extract_team_id(side_key: str) -> Optional[int]:
        side = header.get(side_key) or {}
        if not isinstance(side, dict):
            return None
        tid = side.get("id")
        try:
            # Long → int 캐스팅
            return int(tid) if tid is not None else None
        except (TypeError, ValueError):
            return None

    home_team_id = _extract_team_id("home")
    away_team_id = _extract_team_id("away")

    if not league_id or not season:
        # 리그/시즌이 없으면 standings를 만들 수 없다.
        return None

    try:
        rows: List[Dict[str, Any]] = fetch_all(
            """
            SELECT
                s.rank,
                s.team_id,
                t.name       AS team_name,
                t.logo       AS team_logo,
                s.played,
                s.win,
                s.draw,
                s.lose,
                s.goals_for,
                s.goals_against,
                s.goals_diff,
                s.points,
                s.description,
                s.group_name,
                s.form
            FROM standings AS s
            JOIN teams     AS t ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.season    = %s
            ORDER BY s.group_name, s.rank
            """,
            (league_id, season),
        )
    except Exception:
        # DB 에러가 나더라도 match_detail_bundle 전체가 죽지 않도록 None 리턴
        return None

    if not rows:
        return None

    def _coalesce_int(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    table: List[Dict[str, Any]] = []
    for r in rows:
        team_id = _coalesce_int(r.get("team_id"), 0)

        table.append(
            {
                "position": _coalesce_int(r.get("rank"), 0),
                "team_id": team_id,
                "team_name": r.get("team_name") or "",
                "team_logo": r.get("team_logo"),
                "played": _coalesce_int(r.get("played"), 0),
                "win": _coalesce_int(r.get("win"), 0),
                "draw": _coalesce_int(r.get("draw"), 0),
                "loss": _coalesce_int(r.get("lose"), 0),
                "goals_for": _coalesce_int(r.get("goals_for"), 0),
                "goals_against": _coalesce_int(r.get("goals_against"), 0),
                "goal_diff": _coalesce_int(r.get("goals_diff"), 0),
                "points": _coalesce_int(r.get("points"), 0),
                "description": r.get("description"),
                "group_name": r.get("group_name"),
                "form": r.get("form"),
                "is_home": (home_team_id is not None and team_id == home_team_id),
                "is_away": (away_team_id is not None and team_id == away_team_id),
            }
        )

    return {
        "league": {
            "league_id": league_id,
            "season": season,
            "name": league_name,
        },
        "rows": table,
    }

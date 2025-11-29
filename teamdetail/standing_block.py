# src/teamdetail/standing_block.py

from __future__ import annotations
from typing import Dict, Any, List

from db import fetch_all


def build_standing_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    해당 팀이 속한 리그 standings에서 이 팀이 어떤 위치인지 보여주는 블록.

    - standings 테이블을 사용해서 league_id / season 의 테이블을 가져온다.
    - 팀당 중복 row(스플릿 라운드 등)는 played 가 가장 큰 row만 남긴다.
    - group_name 이 여러 개인 리그(예: MLS 컨퍼런스)는
      이 팀이 속한 group 의 테이블만 내려준다.
    """

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
                s.points,
                s.group_name
            FROM standings AS s
            JOIN teams     AS t ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.season    = %s
            ORDER BY s.rank ASC
            """,
            (league_id, season),
        )
    except Exception:
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

    # 1) 팀당 중복 row 정리 (played 가장 큰 row만 남기기)
    rows_by_team: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        tid = _coalesce_int(r.get("team_id"), 0)
        if tid == 0:
            continue
        prev = rows_by_team.get(tid)
        if prev is None:
            rows_by_team[tid] = r
        else:
            prev_played = _coalesce_int(prev.get("played"), 0)
            cur_played = _coalesce_int(r.get("played"), 0)
            if cur_played > prev_played:
                rows_by_team[tid] = r

    dedup_rows: List[Dict[str, Any]] = list(rows_by_team.values())

    # 2) group_name 이 여러 개인 경우 (예: MLS 컨퍼런스) → 이 팀이 속한 그룹만 사용
    group_names = {
        (r.get("group_name") or "").strip()
        for r in dedup_rows
        if r.get("group_name") is not None
    }
    if len(group_names) > 1:
        main_group = None
        for r in dedup_rows:
            if _coalesce_int(r.get("team_id"), 0) == _coalesce_int(team_id, 0):
                main_group = (r.get("group_name") or "").strip()
                break

        if main_group:
            dedup_rows = [
                r
                for r in dedup_rows
                if (r.get("group_name") or "").strip() == main_group
            ]

    # 3) rank 기준 정렬 후 Kotlin 모델 형태에 맞게 매핑
    dedup_rows.sort(key=lambda r: _coalesce_int(r.get("rank"), 0))

    table: List[Dict[str, Any]] = []
    for r in dedup_rows:
        table.append(
            {
                "position": _coalesce_int(r.get("rank"), 0),
                "team_id": _coalesce_int(r.get("team_id"), 0),
                "team_name": r.get("team_name") or "",
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

# services/matchdetail/standings_block.py

from typing import Any, Dict, Optional, List

from db import fetch_all


def build_standings_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Match Detail용 Standings 블록.

    - league_id / season / home.id / away.id 를 기반으로 standings 테이블 조회.
    - 팀당 중복 row(스플릿 라운드 등)는 played 가 가장 큰 row만 남긴다.
    - group_name 이 여러 개(컨퍼런스 등)면, 우선 home 팀이 속한 그룹
      (없으면 away 팀 그룹)의 테이블만 사용한다.
    """

    league_id = header.get("league_id")
    season = header.get("season")

    league_name = None
    league_info = header.get("league") or {}
    if isinstance(league_info, dict):
        league_name = league_info.get("name")

    def _extract_team_id(side_key: str) -> Optional[int]:
        side = header.get(side_key) or {}
        if not isinstance(side, dict):
            return None
        tid = side.get("id")
        try:
            return int(tid) if tid is not None else None
        except (TypeError, ValueError):
            return None

    home_team_id = _extract_team_id("home")
    away_team_id = _extract_team_id("away")

    if not league_id or not season:
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
        return None

    if not rows:
        return None

    def _coalesce_int(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    # 1) 팀당 중복 row 정리 (played 가장 큰 row만)
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

    # 2) group_name 이 여러 개면, home/away 팀이 속한 group 하나만 사용
    group_names = {
        (r.get("group_name") or "").strip()
        for r in dedup_rows
        if r.get("group_name") is not None
    }
    if len(group_names) > 1:
        main_group = None

        # 먼저 home 팀이 속한 그룹
        if home_team_id is not None:
            for r in dedup_rows:
                if _coalesce_int(r.get("team_id"), 0) == _coalesce_int(home_team_id, 0):
                    main_group = (r.get("group_name") or "").strip()
                    break

        # 없으면 away 팀 기준
        if main_group is None and away_team_id is not None:
            for r in dedup_rows:
                if _coalesce_int(r.get("team_id"), 0) == _coalesce_int(away_team_id, 0):
                    main_group = (r.get("group_name") or "").strip()
                    break

        if main_group:
            dedup_rows = [
                r
                for r in dedup_rows
                if (r.get("group_name") or "").strip() == main_group
            ]

    # 3) position 기준 정렬 후 JSON 매핑
    dedup_rows.sort(key=lambda r: _coalesce_int(r.get("rank"), 0))

    table: List[Dict[str, Any]] = []
    for r in dedup_rows:
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

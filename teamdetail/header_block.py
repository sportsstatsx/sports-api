# src/teamdetail/header_block.py

from __future__ import annotations
from typing import Dict, Any, List

from db import fetch_all  # ✅ home_service 와 같은 DB 헬퍼 사용


def _default_header(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    DB 조회 실패해도 이 기본 형태는 항상 유지되도록 한다.
    """
    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,

        "team_name": None,
        "team_short_name": None,
        "team_logo": None,

        "league_name": None,
        "season_label": str(season),

        "position": None,
        "played": 0,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "goals_for": 0,
        "goals_against": 0,
        "goal_diff": 0,

        # 예: ["W", "D", "L", "W", "W"]
        "recent_form": [],
    }


def build_header_block(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    Team Detail 상단 헤더 영역에 쓸 정보.

    - teams          : 팀명 / 축약명 / 로고
    - leagues        : 리그 이름
    - team_season_stats : 시즌 경기수, 승무패, 득점/실점, 순위
    - match_team_stats  : 최근 폼(W/D/L)

    ⚠️ 테이블 / 컬럼 이름은 실제 스키마에 맞게 필요하면 수정해 줘야 한다.
    """

    header: Dict[str, Any] = _default_header(team_id, league_id, season)

    # ─────────────────────────────────────────────
    # 1) 팀 정보: 이름 / 축약명 / 로고
    # ─────────────────────────────────────────────
    try:
        rows = fetch_all(
            """
            SELECT
                name       AS team_name,
                short_name AS team_short_name,
                logo       AS team_logo
            FROM teams
            WHERE id = %s
            """,
            (team_id,),
        )
        row = rows[0] if rows else None
        if row:
            # fetch_all 이 dict 를 돌려준다고 가정 (home_service 와 동일 패턴)
            header["team_name"] = row.get("team_name")
            header["team_short_name"] = row.get("team_short_name")
            header["team_logo"] = row.get("team_logo")
    except Exception as e:
        print(f"[teamdetail.header_block] team query failed: {e}")

    # ─────────────────────────────────────────────
    # 2) 리그 이름
    # ─────────────────────────────────────────────
    try:
        rows = fetch_all(
            """
            SELECT name AS league_name
            FROM leagues
            WHERE id = %s
            """,
            (league_id,),
        )
        row = rows[0] if rows else None
        if row:
            header["league_name"] = row.get("league_name")
    except Exception as e:
        print(f"[teamdetail.header_block] league query failed: {e}")

    # ─────────────────────────────────────────────
    # 3) 시즌 누적 스탯 (played / WDL / GF / GA / position)
    # ─────────────────────────────────────────────
    try:
        rows = fetch_all(
            """
            SELECT
                played,
                wins,
                draws,
                losses,
                goals_for,
                goals_against,
                position
            FROM team_season_stats
            WHERE team_id  = %s
              AND league_id = %s
              AND season    = %s
            """,
            (team_id, league_id, season),
        )
        row = rows[0] if rows else None
        if row:
            played = row.get("played") or 0
            wins = row.get("wins") or 0
            draws = row.get("draws") or 0
            losses = row.get("losses") or 0
            gf = row.get("goals_for") or 0
            ga = row.get("goals_against") or 0
            pos = row.get("position")

            header["played"] = played
            header["wins"] = wins
            header["draws"] = draws
            header["losses"] = losses
            header["goals_for"] = gf
            header["goals_against"] = ga
            header["goal_diff"] = gf - ga
            if pos is not None:
                header["position"] = pos
    except Exception as e:
        print(f"[teamdetail.header_block] team_season_stats query failed: {e}")

    # ─────────────────────────────────────────────
    # 4) 최근 폼 (최근 10경기 result → W/D/L)
    # ─────────────────────────────────────────────
    try:
        recent_codes: List[str] = []

        rows = fetch_all(
            """
            SELECT result
            FROM match_team_stats
            WHERE team_id  = %s
              AND league_id = %s
              AND season    = %s
            ORDER BY match_date DESC
            LIMIT 10
            """,
            (team_id, league_id, season),
        )

        for row in rows or []:
            # row 가 dict 라고 가정
            result_code = row.get("result")
            if not result_code:
                continue
            code = str(result_code).upper()
            if code in ("W", "D", "L"):
                recent_codes.append(code)

        header["recent_form"] = recent_codes
    except Exception as e:
        print(f"[teamdetail.header_block] recent_form query failed: {e}")

    return header

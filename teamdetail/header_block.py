# src/teamdetail/header_block.py

from __future__ import annotations
from typing import Dict, Any, List

import json
from db import fetch_all  # home_service 와 같은 DB 헬퍼 사용


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

        "position": None,  # 나중에 standings 에서 가져오고 싶으면 추가
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

    - teams               : 팀명 / 로고
    - leagues             : 리그 이름
    - team_season_stats   : 시즌 경기수, 승무패, 득점/실점, 최근 폼(form)
    """

    header: Dict[str, Any] = _default_header(team_id, league_id, season)

    # ─────────────────────────────────────────────
    # 1) 팀 정보: 이름 / 로고
    #    테이블: public.teams(id, name, country, logo)
    # ─────────────────────────────────────────────
    try:
        rows = fetch_all(
            """
            SELECT
                name,
                logo
            FROM teams
            WHERE id = %s
            """,
            (team_id,),
        )
        row = rows[0] if rows else None
        if row:
            header["team_name"] = row.get("name")
            # short_name 은 별도 컬럼이 없으니, 일단 팀명 그대로 쓰거나
            # 나중에 team_name_key 테이블 생기면 거기서 가져오자.
            header["team_short_name"] = row.get("name")
            header["team_logo"] = row.get("logo")
    except Exception as e:
        print(f"[teamdetail.header_block] team query failed: {e}")

    # ─────────────────────────────────────────────
    # 2) 리그 이름
    #    테이블: public.leagues(id, name, country, logo, flag)
    # ─────────────────────────────────────────────
    try:
        rows = fetch_all(
            """
            SELECT name
            FROM leagues
            WHERE id = %s
            """,
            (league_id,),
        )
        row = rows[0] if rows else None
        if row:
            header["league_name"] = row.get("name")
    except Exception as e:
        print(f"[teamdetail.header_block] league query failed: {e}")

    # ─────────────────────────────────────────────
    # 3) 시즌 누적 스탯 + 최근 폼
    #    테이블: public.team_season_stats
    #    컬럼 : league_id, season, team_id, name, value
    #    - name='full_json' 인 row 의 value 가 API-Football team stats 전체 JSON
    # ─────────────────────────────────────────────
    try:
        rows = fetch_all(
            """
            SELECT value
            FROM team_season_stats
            WHERE league_id = %s
              AND season    = %s
              AND team_id   = %s
              AND name      = 'full_json'
            """,
            (league_id, season, team_id),
        )
        row = rows[0] if rows else None
        if row:
            raw_json = row.get("value")
            if isinstance(raw_json, str) and raw_json:
                data = json.loads(raw_json)

                # --- fixtures / wins / draws / loses / played ---
                fixtures = (data.get("fixtures") or {})
                played_total = ((fixtures.get("played") or {}).get("total")) or 0
                wins_total = ((fixtures.get("wins") or {}).get("total")) or 0
                draws_total = ((fixtures.get("draws") or {}).get("total")) or 0
                loses_total = ((fixtures.get("loses") or {}).get("total")) or 0

                header["played"] = int(played_total)
                header["wins"] = int(wins_total)
                header["draws"] = int(draws_total)
                header["losses"] = int(loses_total)

                # --- goals for/against ---
                goals = (data.get("goals") or {})
                goals_for_total = (
                    ((goals.get("for") or {}).get("total") or {}).get("total")
                ) or 0
                goals_against_total = (
                    ((goals.get("against") or {}).get("total") or {}).get("total")
                ) or 0

                header["goals_for"] = int(goals_for_total)
                header["goals_against"] = int(goals_against_total)
                header["goal_diff"] = int(goals_for_total) - int(goals_against_total)

                # --- recent_form: "WDLLWW..." 문자열 -> ["W","D","L",...]
                form_str = (data.get("form") or "").upper()
                # W, D, L 문자만 추출
                codes: List[str] = [c for c in form_str if c in ("W", "D", "L")]
                # 최근 경기부터 앞쪽일 가능성이 높으니, 앞에서 최대 10개만 사용
                header["recent_form"] = codes[:10]
    except Exception as e:
        print(f"[teamdetail.header_block] team_season_stats(full_json) parse failed: {e}")

    # position(순위)는 standings 테이블에서 가져올 수 있지만,
    # 지금 UI 에서는 꼭 필요하지 않으니 일단 생략.
    # 나중에 필요하면 standings 에서 team_id 매칭해서 rank 만 추가하면 됨.

    return header

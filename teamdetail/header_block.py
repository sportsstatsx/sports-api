# src/teamdetail/header_block.py

from __future__ import annotations
from typing import Dict, Any, List

from db import get_db  # 프로젝트에서 쓰는 DB 헬퍼 (MatchDetail 쪽과 동일하게 사용한다고 가정)


def _default_header(team_id: int, league_id: int, season: int) -> Dict[str, Any]:
    """
    기본 값 (DB 조회 실패해도 이 형태는 항상 유지되도록)
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

        "position": None,        # 리그 순위(선택)
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

    - teams / leagues / team_season_stats / (matches or match_team_stats) 를 사용해서
      팀명, 리그명, 시즌 기록, 최근 폼을 채운다.

    ⚠️ 테이블/컬럼 이름은 프로젝트 스키마에 맞게 조정 필요할 수 있음.
    """

    header: Dict[str, Any] = _default_header(team_id, league_id, season)

    conn = get_db()
    with conn.cursor() as cur:
        # ─────────────────────────────────────────────
        # 1) 팀 정보: 이름 / 축약명 / 로고
        # ─────────────────────────────────────────────
        try:
            cur.execute(
                """
                SELECT name, short_name, logo
                FROM teams
                WHERE id = %s
                """,
                (team_id,),
            )
            row = cur.fetchone()
            if row:
                header["team_name"] = row[0]
                header["team_short_name"] = row[1]
                header["team_logo"] = row[2]
        except Exception as e:
            # 테이블 이름이 다를 수도 있으니, 일단 서비스 죽지 않게만.
            print(f"[teamdetail.header_block] team query failed: {e}")

        # ─────────────────────────────────────────────
        # 2) 리그 이름
        # ─────────────────────────────────────────────
        try:
            cur.execute(
                """
                SELECT name
                FROM leagues
                WHERE id = %s
                """,
                (league_id,),
            )
            row = cur.fetchone()
            if row:
                header["league_name"] = row[0]
        except Exception as e:
            print(f"[teamdetail.header_block] league query failed: {e}")

        # ─────────────────────────────────────────────
        # 3) 시즌 누적 스탯 (played / WDL / GF / GA)
        # ─────────────────────────────────────────────
        try:
            cur.execute(
                """
                SELECT played, wins, draws, losses, goals_for, goals_against, position
                FROM team_season_stats
                WHERE team_id = %s
                  AND league_id = %s
                  AND season = %s
                """,
                (team_id, league_id, season),
            )
            row = cur.fetchone()
            if row:
                played, wins, draws, losses, gf, ga, pos = row
                header["played"] = played or 0
                header["wins"] = wins or 0
                header["draws"] = draws or 0
                header["losses"] = losses or 0
                header["goals_for"] = gf or 0
                header["goals_against"] = ga or 0
                header["goal_diff"] = (gf or 0) - (ga or 0)
                if pos is not None:
                    header["position"] = pos
        except Exception as e:
            print(f"[teamdetail.header_block] team_season_stats query failed: {e}")

        # ─────────────────────────────────────────────
        # 4) 최근 폼 (마지막 N경기 결과 → W/D/L)
        #    MatchDetail 의 form_block 과 비슷한 로직으로 구현
        # ─────────────────────────────────────────────
        try:
            recent_codes: List[str] = []

            # 예시 스키마: match_team_stats 테이블에서 결과코드(result: 'W','D','L')와
            # 경기일자(kickoff_time) 기준으로 최근 경기 N개를 가져온다고 가정.
            cur.execute(
                """
                SELECT result
                FROM match_team_stats
                WHERE team_id = %s
                  AND league_id = %s
                  AND season = %s
                ORDER BY match_date DESC
                LIMIT 10
                """,
                (team_id, league_id, season),
            )
            rows = cur.fetchall() or []
            for (result_code,) in rows:
                if not result_code:
                    continue
                code = str(result_code).upper()
                if code in ("W", "D", "L"):
                    recent_codes.append(code)

            header["recent_form"] = recent_codes
        except Exception as e:
            print(f"[teamdetail.header_block] recent_form query failed: {e}")

    return header

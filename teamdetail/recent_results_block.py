# src/teamdetail/recent_results_block.py

from __future__ import annotations
from typing import Dict, Any, List

from db import fetch_all


def _build_result_code(
    team_id: int,
    home_id: int,
    away_id: int,
    home_ft: int | None,
    away_ft: int | None,
) -> str | None:
    """
    해당 team_id 기준으로 W/D/L 코드 계산.
    """
    if home_ft is None or away_ft is None:
        return None

    if home_ft == away_ft:
        return "D"

    is_home = team_id == home_id
    team_goals = home_ft if is_home else away_ft
    opp_goals = away_ft if is_home else home_ft

    return "W" if team_goals > opp_goals else "L"


def build_recent_results_block(
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    """
    최근 경기 결과 리스트 블록.

    - matches 테이블에서 해당 시즌, 해당 팀이 출전한 경기들을 가져온다.
    - 리그/대륙컵 구분 없이 시즌 전체 기준 (header_recent_form 과 동일한 느낌).
    - 종료된 경기(home_ft/away_ft 있는 것만).
    - 최신 경기부터 정렬해서 내려준다 (앱에서 최근 5경기만 보여주고,
      Show all 누르면 전체 rows 를 사용).
    """

    rows_db = fetch_all(
        """
        SELECT
            m.id AS fixture_id,     -- 내부 PK (앱에서는 fixture_id 로 사용)
            m.league_id,
            m.season,
            m.date_utc,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            th.name AS home_team_name,
            ta.name AS away_team_name
        FROM matches AS m
        JOIN teams AS th ON th.id = m.home_id
        JOIN teams AS ta ON ta.id = m.away_id
        WHERE m.season = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND m.home_ft IS NOT NULL
          AND m.away_ft IS NOT NULL
        ORDER BY m.date_utc DESC
        """,
        (season, team_id, team_id),
    )

    rows: List[Dict[str, Any]] = []

    for r in rows_db:
        home_id = r["home_id"]
        away_id = r["away_id"]
        home_ft = r["home_ft"]
        away_ft = r["away_ft"]

        result_code = _build_result_code(
            team_id=team_id,
            home_id=home_id,
            away_id=away_id,
            home_ft=home_ft,
            away_ft=away_ft,
        )

        date_utc = r.get("date_utc")
        # datetime 이든 문자열이든 일단 str() 로 직렬화
        date_str = str(date_utc) if date_utc is not None else None

        rows.append(
            {
                "fixture_id": r["fixture_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "date_utc": date_str,
                "home_team_name": r["home_team_name"],
                "away_team_name": r["away_team_name"],
                "home_goals": home_ft,
                "away_goals": away_ft,
                "result_code": result_code,
            }
        )

    return {
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "rows": rows,
    }

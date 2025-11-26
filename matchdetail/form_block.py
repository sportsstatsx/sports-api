# services/matchdetail/form_block.py

from typing import Any, Dict, List, Tuple

from db import fetch_all


def _safe_int(val: Any) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _extract_team_ids(header: Dict[str, Any]) -> Tuple[int, int]:
    """
    header에서 home/away 팀 ID를 최대한 유연하게 뽑는 헬퍼.

    현재 header 예시:
      'home': {'id': 40, ...}
      'away': {'id': 65, ...}

    과거/다른 형태도 같이 대응:
      'home_team_id': 40
      'home_team': {'id': 40, ...}
    """

    # 1) 가장 먼저 home_team_id / away_team_id 키를 시도
    home_id = header.get("home_team_id")
    away_id = header.get("away_team_id")

    # 2) header["home"]["id"] / header["away"]["id"] 형태 지원
    if home_id is None:
        home = header.get("home") or {}
        home_id = home.get("id")
    if away_id is None:
        away = header.get("away") or {}
        away_id = away.get("id")

    # 3) 혹시 header["home_team"]["id"] 형태도 있을 수 있으니 백업
    if home_id is None:
        home_team = header.get("home_team") or {}
        home_id = home_team.get("id")
    if away_id is None:
        away_team = header.get("away_team") or {}
        away_id = away_team.get("id")

    return _safe_int(home_id), _safe_int(away_id)


def _build_team_form(
    header: Dict[str, Any],
    team_side: str,  # "home" or "away"
    limit: int = 5,
) -> Tuple[List[str], int, int]:
    """
    실제 최근 폼(W/D/L)과 득점/실점 계산 로직.

    반환:
      - last_results: 예) ["W", "D", "L", "W", "W"]  (최신 경기부터 최대 5개)
      - goals_for:    최근 limit 경기 총 득점
      - goals_against:최근 limit 경기 총 실점
    """
    home_team_id, away_team_id = _extract_team_ids(header)
    if team_side == "home":
        team_id = home_team_id
    else:
        team_id = away_team_id

    if team_id <= 0:
        # 필수 정보가 없으면 빈 값 반환
        return [], 0, 0

    # 해당 팀이 참가한 "종료된 경기" 중 가장 최근 경기들을 기준으로 폼 계산
    rows = fetch_all(
        """
        SELECT
            m.date_utc,
            m.home_id,
            m.away_id,
            m.goals_home,
            m.goals_away
        FROM matches m
        WHERE (m.home_id = %s OR m.away_id = %s)
          AND m.goals_home IS NOT NULL
          AND m.goals_away IS NOT NULL
        ORDER BY m.date_utc DESC
        LIMIT %s
        """,
        (team_id, team_id, limit * 2),
    )

    last_results: List[str] = []
    goals_for = 0
    goals_against = 0

    for r in rows:
        gh = r.get("goals_home")
        ga = r.get("goals_away")

        # 방어: 혹시라도 None 섞여 있으면 스킵
        if gh is None or ga is None:
            continue

        home_id = _safe_int(r.get("home_id"))
        away_id = _safe_int(r.get("away_id"))

        # 이 팀 기준으로 득점/실점 방향 결정
        if home_id == team_id:
            gf = _safe_int(gh)
            ga_ = _safe_int(ga)
        elif away_id == team_id:
            gf = _safe_int(ga)
            ga_ = _safe_int(gh)
        else:
            # where 조건과 안 맞는 경우 방어적으로 스킵
            continue

        # W / D / L 결정
        if gf > ga_:
            res = "W"
        elif gf < ga_:
            res = "L"
        else:
            res = "D"

        last_results.append(res)
        goals_for += gf
        goals_against += ga_

        # 원하는 개수(limit)에 도달하면 멈춘다.
        if len(last_results) >= limit:
            break

    return last_results, goals_for, goals_against


def build_form_block(header: Dict[str, Any]) -> Dict[str, Any]:
    """
    Match Detail 상단 ScoreBlock 에서 사용할 '최근 폼' 블록.

    최종적으로 이런 구조로 내려간다:

    {
        "home_last5": ["W","D","L","W","W"],
        "away_last5": ["L","L","W","D","W"],
        "home_goals_for": 8,
        "home_goals_against": 3,
        "away_goals_for": 5,
        "away_goals_against": 7,
    }
    """

    home_last5, home_gf, home_ga = _build_team_form(header, team_side="home", limit=5)
    away_last5, away_gf, away_ga = _build_team_form(header, team_side="away", limit=5)

    return {
        "home_last5": home_last5,
            "away_last5": away_last5,
        "home_goals_for": home_gf,
        "home_goals_against": home_ga,
        "away_goals_for": away_gf,
        "away_goals_against": away_ga,
    }

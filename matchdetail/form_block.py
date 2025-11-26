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
    header 구조가 약간 달라도 최대한 안전하게
    home / away 팀 ID 를 뽑아내기 위한 헬퍼.
    """
    # 가장 단순한 형태: header["home_team_id"], header["away_team_id"]
    home_id = header.get("home_team_id")
    away_id = header.get("away_team_id")

    # 혹시 header["home_team"]["id"] 형태일 수도 있어서 대비
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
    league_id = _safe_int(header.get("league_id"))
    season = _safe_int(header.get("season"))

    home_team_id, away_team_id = _extract_team_ids(header)
    if team_side == "home":
        team_id = home_team_id
    else:
        team_id = away_team_id

    if league_id <= 0 or season <= 0 or team_id <= 0:
        # 필수 정보가 없으면 빈 값 반환
        return [], 0, 0

    # matches 테이블에서 해당 리그/시즌/팀의 최근 경기들을 가져온다.
    # 여기서는 status_short 같은 컬럼에 의존하지 않고,
    # 득점/실점 컬럼(goals_home/goals_away)이 있는 경기만 사용해서
    # '종료된 경기'로 간주한다.
    rows = fetch_all(
        """
        SELECT
            m.date_utc,
            m.home_id,
            m.away_id,
            m.goals_home,
            m.goals_away
        FROM matches m
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
        ORDER BY m.date_utc DESC
        LIMIT %s
        """,
        (league_id, season, team_id, team_id, limit * 2),
    )

    last_results: List[str] = []
    goals_for = 0
    goals_against = 0

    for r in rows:
        gh = r.get("goals_home")
        ga = r.get("goals_away")

        # 골 정보가 없는(아직 안 끝난) 경기는 스킵
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
            # 방어코드: 혹시라도 where 조건과 불일치하는 row 가 들어온 경우
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

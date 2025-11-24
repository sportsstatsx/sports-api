# services/matchdetail/stats_block.py

from typing import Any, Dict, Optional

from db import fetch_all


def _extract_fixture_id(header: Dict[str, Any]) -> Optional[int]:
    """
    header 블록에서 fixture_id 를 최대한 안전하게 추출.
    header_block 구현에 따라 키가 조금씩 다를 수 있으니 여러 패턴을 지원.
    """
    # 1) 가장 단순한 케이스
    if "fixture_id" in header and header["fixture_id"] is not None:
        return int(header["fixture_id"])

    # 2) 혹시 fixture 객체 안에 들어있는 형태라면
    fixture = header.get("fixture") or {}
    if isinstance(fixture, dict):
        if "fixture_id" in fixture and fixture["fixture_id"] is not None:
            return int(fixture["fixture_id"])
        if "id" in fixture and fixture["id"] is not None:
            return int(fixture["id"])

    # 3) 마지막 fallback
    return None


def _extract_team_ids(header: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """
    header 블록에서 home/away 팀 id 를 최대한 안전하게 추출.
    """
    home_id: Optional[int] = None
    away_id: Optional[int] = None

    # 1) 직관적인 키들 먼저
    if "home_team_id" in header:
        try:
            home_id = int(header["home_team_id"])
        except (TypeError, ValueError):
            home_id = None
    if "away_team_id" in header:
        try:
            away_id = int(header["away_team_id"])
        except (TypeError, ValueError):
            away_id = None

    # 2) teams/home, teams/away 형태
    teams = header.get("teams") or {}
    if isinstance(teams, dict):
        home_info = teams.get("home") or {}
        away_info = teams.get("away") or {}
        if home_id is None and isinstance(home_info, dict):
            tid = home_info.get("team_id") or home_info.get("id")
            if tid is not None:
                try:
                    home_id = int(tid)
                except (TypeError, ValueError):
                    pass
        if away_id is None and isinstance(away_info, dict):
            tid = away_info.get("team_id") or away_info.get("id")
            if tid is not None:
                try:
                    away_id = int(tid)
                except (TypeError, ValueError):
                    pass

    return {"home_team_id": home_id, "away_team_id": away_id}


def _safe_get(row: Dict[str, Any], key: str) -> Any:
    """
    row 딕셔너리에서 key 가 없으면 None 반환.
    match_team_stats 컬럼 이름이 조금씩 달라도 터지지 않게 방어적으로 처리.
    """
    return row[key] if key in row else None


def _build_team_stats_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    match_team_stats 한 row 를 기반으로, UI에서 쓰기 좋은 형태의 팀 스탯 딕셔너리 생성.

    컬럼 이름은 프로젝트마다 차이가 있을 수 있으므로,
    존재 여부를 체크해서 있으면 그대로 사용, 없으면 None 으로 둔다.
    필요하면 나중에 여기 컬럼 매핑만 수정해도 됨.
    """
    return {
        "team_id": _safe_get(row, "team_id"),
        # 팀 이름 컬럼이 있으면 사용 (없으면 헤더 쪽에서 팀 이름을 가져다 쓰면 됨)
        "team_name": _safe_get(row, "team_name"),
        # 슈팅 관련
        "shots_total": _safe_get(row, "shots_total") or _safe_get(row, "shots_all"),
        "shots_on_target": _safe_get(row, "shots_on_target") or _safe_get(
            row, "shots_on_goal"
        ),
        "shots_off_target": _safe_get(row, "shots_off_target") or _safe_get(
            row, "shots_off_goal"
        ),
        "shots_blocked": _safe_get(row, "shots_blocked") or _safe_get(
            row, "blocked_shots"
        ),
        # 패스 / 패스 성공률
        "passes": _safe_get(row, "passes") or _safe_get(row, "total_passes"),
        "pass_accuracy": _safe_get(row, "pass_accuracy") or _safe_get(
            row, "passes_accuracy"
        ),
        # 점유율
        "possession": _safe_get(row, "possession")
        or _safe_get(row, "ball_possession"),
        # 세트피스 / 파울
        "corners": _safe_get(row, "corners"),
        "offsides": _safe_get(row, "offsides"),
        "fouls": _safe_get(row, "fouls"),
        "yellow_cards": _safe_get(row, "yellow_cards"),
        "red_cards": _safe_get(row, "red_cards"),
        "goalkeeper_saves": _safe_get(row, "goalkeeper_saves"),
        # xG 류 (있으면 사용)
        "xg": _safe_get(row, "xg") or _safe_get(row, "expected_goals"),
        # 필요하면 나중에 더 추가 (shots_inside_box, shots_outside_box 등)
    }


def build_stats_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    match_team_stats / match_player_stats 기반 팀/선수 스탯 구현.

    1단계: 팀 스탯만 구현 (match_team_stats).
    나중에 필요하면 match_player_stats 기반 선수 스탯 섹션을 확장.
    """
    fixture_id = _extract_fixture_id(header)
    if fixture_id is None:
        # fixture_id 가 없으면 스탯을 만들 수 없음
        return None

    team_ids = _extract_team_ids(header)
    home_team_id = team_ids["home_team_id"]
    away_team_id = team_ids["away_team_id"]

    # 1) match_team_stats 에서 이 fixture 의 팀 스탯 조회
    #    컬럼 이름이 프로젝트마다 약간 다를 수 있으므로 SELECT * 후에 Python 쪽에서 매핑
    sql = """
        SELECT *
        FROM match_team_stats
        WHERE fixture_id = %s
    """
    rows = fetch_all(sql, (fixture_id,))

    if not rows:
        # 해당 경기의 팀 스탯이 아직 수집되지 않았으면 None 반환
        return None

    home_block: Optional[Dict[str, Any]] = None
    away_block: Optional[Dict[str, Any]] = None

    for row in rows:
        team_id = _safe_get(row, "team_id")
        stats = _build_team_stats_from_row(row)

        # home / away 매핑
        if home_team_id is not None and team_id == home_team_id:
            home_block = stats
        elif away_team_id is not None and team_id == away_team_id:
            away_block = stats
        else:
            # header 에서 home/away 를 못 찾았거나 team_id 가 안 맞을 때:
            # 아직 비어 있는 쪽에 순서대로 채워 넣는 fallback 처리
            if home_block is None:
                home_block = stats
            elif away_block is None:
                away_block = stats

    return {
        "home": home_block,
        "away": away_block,
        # 2단계에서 선수 스탯(match_player_stats) 섹션을 붙이고 싶다면,
        # 여기 예: "players": {"home": [...], "away": [...]} 형태로 확장 가능.
    }

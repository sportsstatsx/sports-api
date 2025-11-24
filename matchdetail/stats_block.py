# services/matchdetail/stats_block.py
"""
Match Detail – Stats 블록 빌더

A방식:
  - 매치디테일 번들(/api/match_detail_bundle) 안에서
    "stats": { ... }
  블록을 만들어주는 역할만 담당한다.

1단계 구현 범위
  - match_team_stats 기준 팀 스탯만 내려준다.
  - 선수별 스탯(match_player_stats)은 이후 단계에서 확장한다.

응답 구조 예시 (Python dict 기준):

  {
      "team": {
          "home_team_id": 40,
          "away_team_id": 41,
          "home": { ... match_team_stats row ... } | null,
          "away": { ... match_team_stats row ... } | null,
          "rows": [ { ... }, { ... } ]   # 원본 row 전체 (디버깅/향후 확장용)
      },
      "players": null
  }

Kotlin 쪽 StatsBlock:
  data class StatsBlock(
      val team: Any? = null,
      val players: Any? = null
  )

과 그대로 호환된다.
"""

from typing import Any, Dict, Optional

from db import fetch_all


def _get_fixture_id_from_header(header: Dict[str, Any]) -> Optional[int]:
    """
    header 블록에서 fixture_id 를 최대한 단순하게 추출.
    HeaderBlock 정의:
      fixture_id: Long,
      league_id: Long,
      season: Int,
      home: { id: Long, ... },
      away: { id: Long, ... }

    을 그대로 따른다고 가정한다.
    """
    raw = header.get("fixture_id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _get_team_ids_from_header(header: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """
    header.home.id / header.away.id 를 읽어서 반환.
    """
    home_id: Optional[int] = None
    away_id: Optional[int] = None

    home = header.get("home") or {}
    away = header.get("away") or {}

    if isinstance(home, dict):
        try:
            home_id = int(home.get("id")) if home.get("id") is not None else None
        except (TypeError, ValueError):
            home_id = None

    if isinstance(away, dict):
        try:
            away_id = int(away.get("id")) if away.get("id") is not None else None
        except (TypeError, ValueError):
            away_id = None

    return {"home_team_id": home_id, "away_team_id": away_id}


def build_stats_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    match_team_stats 기반 팀 스탯 블록 생성.

    - header 에서 fixture_id / home_id / away_id 를 읽는다.
    - match_team_stats 에서 해당 fixture 의 모든 row 를 SELECT * 한다.
      (컬럼 스키마는 추후 필요 시 구체적으로 매핑 가능)
    - home/away team_id 와 매칭해서 home / away 슬롯에 채워 넣는다.
    - 아무 데이터도 없으면 None 반환.
    """
    fixture_id = _get_fixture_id_from_header(header)
    if fixture_id is None:
        # fixture_id 없으면 스탯을 만들 수 없다
        return None

    team_ids = _get_team_ids_from_header(header)
    home_team_id = team_ids["home_team_id"]
    away_team_id = team_ids["away_team_id"]

    # match_team_stats 에서 이 경기의 팀 스탯 전부 읽기
    # 컬럼 이름은 SELECT * 로 그대로 가져오고,
    # Python dict 그대로를 JSON 으로 내려보내는 방식으로 1차 구현.
    sql = """
        SELECT *
        FROM match_team_stats
        WHERE fixture_id = %s
    """
    rows = fetch_all(sql, (fixture_id,))

    if not rows:
        # 아직 이 경기의 스탯이 수집되지 않았을 수 있음
        return None

    home_block: Optional[Dict[str, Any]] = None
    away_block: Optional[Dict[str, Any]] = None

    for row in rows:
        # team_id 컬럼이 있다는 가정 하에, home/away 매칭을 시도한다.
        team_id = row.get("team_id")

        if team_id is not None:
            # header 의 home/away 팀 id 와 우선 매칭
            try:
                tid = int(team_id)
            except (TypeError, ValueError):
                tid = None

            if tid is not None:
                if home_team_id is not None and tid == home_team_id:
                    home_block = row
                    continue
                if away_team_id is not None and tid == away_team_id:
                    away_block = row
                    continue

        # 혹시 header 에 팀 id 가 없거나 매칭이 안 되는 경우,
        # 아직 비어 있는 쪽에 순서대로 채워 넣는 fallback 처리
        if home_block is None:
            home_block = row
        elif away_block is None:
            away_block = row

    team_block: Dict[str, Any] = {
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home": home_block,
        "away": away_block,
        # 디버깅/향후 확장을 위한 전체 row 목록
        "rows": rows,
    }

    # 선수 스탯은 이후 단계에서 match_player_stats 를 붙이는 형태로 확장 예정
    players_block: Optional[Dict[str, Any]] = None

    return {
        "team": team_block,
        "players": players_block,
    }
